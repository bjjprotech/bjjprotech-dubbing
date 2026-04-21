"""
BJJ Pro Tech — FFmpeg Server para Railway
Recebe jobs via HTTP, processa com FFmpeg, re-faz upload no Bunny.
"""

import os, json, base64, subprocess, tempfile, threading, time, requests
from pathlib import Path
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
# Aumenta limite de requisição para 200MB (WAVs grandes em base64)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024
CORS(app)  # Permite chamadas de qualquer origem (browser → Railway)

JOBS = {}
JOBS_LOCK = threading.Lock()

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def update_job(job_id, **kwargs):
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(kwargs)

def download_file(url, dest, headers=None):
    r = requests.get(url, headers=headers or {}, stream=True, timeout=600)
    r.raise_for_status()
    total = int(r.headers.get('content-length', 0))
    done = 0
    with open(dest, 'wb') as f:
        for chunk in r.iter_content(1024 * 1024):
            f.write(chunk)
            done += len(chunk)
            if total:
                log(f"  Download: {done/total*100:.1f}% ({done//1024//1024}MB/{total//1024//1024}MB)")
    return done

# Serve merged video temporarily for Bunny Fetch
# We use a simple in-memory store: {token: (filepath, keep_alive)}
TEMP_FILES = {}
TEMP_FILES_LOCK = threading.Lock()

def serve_temp_file(file_path):
    """Registra arquivo para servir temporariamente e retorna token único."""
    import uuid
    token = str(uuid.uuid4()).replace('-','')
    with TEMP_FILES_LOCK:
        TEMP_FILES[token] = str(file_path)
    return token

def upload_to_bunny(video_guid, library_id, api_key, file_path, video_title):
    """
    Estratégia: Railway serve o vídeo mesclado temporariamente →
    Bunny Fetch API baixa da URL do Railway e substitui o vídeo
    no mesmo GUID → GUID preservado, player embed intacto.
    """
    base = f"https://video.bunnycdn.com/library/{library_id}"
    file_size = os.path.getsize(file_path)
    log(f"  Vídeo mesclado: {file_size//1024//1024}MB")

    # 1. Pegar URL pública do Railway
    railway_url = os.environ.get('RAILWAY_PUBLIC_DOMAIN', '')
    if not railway_url:
        railway_url = os.environ.get('RAILWAY_STATIC_URL', '')
    if not railway_url:
        raise RuntimeError("RAILWAY_PUBLIC_DOMAIN não configurado nas variáveis do Railway.")
    if not railway_url.startswith('http'):
        railway_url = f"https://{railway_url}"

    # 2. Registrar arquivo para servir
    token = serve_temp_file(file_path)
    temp_url = f"{railway_url}/temp/{token}"
    log(f"  Servindo temporariamente em: {temp_url}")

    try:
        # 3. Chamar Bunny Fetch no GUID existente — substitui o arquivo preservando ID
        log(f"  Chamando Bunny Fetch no GUID {video_guid[:8]} (preserva embed)...")
        r = requests.post(
            f"{base}/videos/{video_guid}/fetch",
            headers={
                "AccessKey": api_key,
                "Content-Type": "application/json",
                "accept": "application/json"
            },
            json={"url": temp_url},
            timeout=60
        )

        log(f"  Bunny Fetch resposta: HTTP {r.status_code} — {r.text[:150]}")

        if r.status_code not in (200, 201, 202):
            # Se fetch falhar, tenta upload direto como fallback
            log(f"  Fetch falhou, tentando upload direto como fallback...")
            headers_bin = {"AccessKey": api_key, "Content-Type": "application/octet-stream"}
            with open(file_path, 'rb') as f:
                r2 = requests.put(f"{base}/videos/{video_guid}", headers=headers_bin, data=f, timeout=3600)
            if r2.status_code not in (200, 201):
                raise RuntimeError(f"Upload direto também falhou: HTTP {r2.status_code} — {r2.text[:200]}")
            log(f"  Upload direto OK como fallback")
        else:
            log(f"  Bunny Fetch aceito — GUID {video_guid} preservado!")
            # Aguarda Bunny começar o download antes de limpar o arquivo
            log(f"  Aguardando Bunny iniciar download (60s)...")
            time.sleep(60)

    finally:
        # Limpa arquivo temporário
        with TEMP_FILES_LOCK:
            TEMP_FILES.pop(token, None)
        log(f"  Arquivo temporário removido")

    return video_guid

def upload_caption(video_guid, library_id, api_key, lang, label, srt_content):
    b64 = base64.b64encode(srt_content.encode('utf-8')).decode('ascii')
    url = f"https://video.bunnycdn.com/library/{library_id}/videos/{video_guid}/captions/{lang}"
    r = requests.post(url,
        headers={"AccessKey": api_key, "Content-Type": "application/json"},
        json={"srclang": lang, "label": label, "captionsFile": b64},
        timeout=30)
    return r.status_code in (200, 201)

def enable_multi_audio(library_id, api_key):
    """
    Ativa Multi Audio Track Support.
    Nota: este endpoint requer a API Key da CONTA (não da biblioteca).
    Se der 401, o multi-audio precisa ser ativado manualmente no painel Bunny:
    Dashboard → Stream → sua biblioteca → Encoding → Enable Multi Audio Track Support
    """
    try:
        # Try with stream library key first
        r = requests.post(
            f"https://api.bunny.net/videolibrary/{library_id}",
            headers={
                "AccessKey": api_key,
                "Content-Type": "application/json",
                "accept": "application/json"
            },
            json={"EnableMultiAudioTrackSupport": True}, timeout=15)
        log(f"  Multi Audio Track: HTTP {r.status_code}")
        if r.status_code == 401:
            log(f"  AVISO: Multi Audio precisa ser ativado manualmente no painel Bunny")
            log(f"  Dashboard → Stream → biblioteca → Encoding → Multi Audio Track Support")
    except Exception as e:
        log(f"  Multi Audio Track warning: {e}")

def parse_srt_timestamps(srt_content):
    """Parse SRT and return list of (start_ms, end_ms, text)"""
    import re
    segments = []
    blocks = re.split(r'\n\n+', srt_content.strip())
    for block in blocks:
        lines = block.strip().split('\n')
        if len(lines) < 3: continue
        m = re.match(r'(\d+):(\d+):(\d+)[,\.](\d+)\s*-->\s*(\d+):(\d+):(\d+)[,\.](\d+)', lines[1])
        if not m: continue
        def to_ms(h,m,s,ms): return int(h)*3600000 + int(m)*60000 + int(s)*1000 + int(ms)
        start = to_ms(*m.groups()[:4])
        end   = to_ms(*m.groups()[4:])
        text  = ' '.join(lines[2:])
        segments.append((start, end, text))
    return segments

def sync_audio_to_srt(wav_path, srt_content, tmp, lang):
    """
    Syncs dubbed WAV to SRT timestamps using FFmpeg.
    The dubbed audio (text corrido) is split into segments matching SRT timing.
    Each segment is placed at the correct timestamp with silence between.
    """
    import math
    segments = parse_srt_timestamps(srt_content)
    if not segments: return None

    total_ms = segments[-1][1] + 500
    total_s  = total_ms / 1000.0

    # Get duration of dubbed audio
    probe = subprocess.run([
        'ffprobe', '-v', 'quiet', '-print_format', 'json',
        '-show_streams', str(wav_path)
    ], capture_output=True, text=True)

    import json as _json
    try:
        probe_data = _json.loads(probe.stdout)
        dub_duration_s = float(probe_data['streams'][0]['duration'])
    except:
        log(f"  Sync {lang}: não foi possível determinar duração, usando sem sync")
        return None

    # Total text duration from SRT
    text_total_ms = sum(e - s for s, e, _ in segments)
    text_total_s  = text_total_ms / 1000.0

    if text_total_s <= 0: return None

    # Build FFmpeg filter to place audio segments at correct timestamps
    # Strategy: split dubbed audio proportionally by segment text length
    # and place each piece at the correct SRT timestamp
    total_chars = sum(len(t) for _, _, t in segments)
    if total_chars == 0: return None

    # Create silent base track
    silence_path = tmp / f'silence_{lang}.wav'
    result = subprocess.run([
        'ffmpeg', '-y',
        '-f', 'lavfi', '-i', f'anullsrc=channel_layout=mono:sample_rate=44100',
        '-t', str(total_s + 1),
        str(silence_path)
    ], capture_output=True, timeout=60)
    if result.returncode != 0: return None

    # Extract segments from dubbed audio based on proportional timing
    segment_files = []
    current_pos_s = 0.0
    for i, (start_ms, end_ms, text) in enumerate(segments):
        char_ratio = len(text) / total_chars
        seg_dur_s  = dub_duration_s * char_ratio

        seg_path = tmp / f'seg_{lang}_{i}.wav'
        result = subprocess.run([
            'ffmpeg', '-y',
            '-i', str(wav_path),
            '-ss', str(current_pos_s),
            '-t',  str(seg_dur_s),
            '-ar', '44100', '-ac', '1',
            str(seg_path)
        ], capture_output=True, timeout=60)

        if result.returncode == 0:
            segment_files.append((start_ms / 1000.0, seg_path))

        current_pos_s += seg_dur_s

    if not segment_files: return None

    # Mix all segments into the silence base at correct timestamps
    # Build amix filter
    inputs = ['-i', str(silence_path)]
    for _, seg_path in segment_files:
        inputs += ['-i', str(seg_path)]

    filter_parts = []
    for i, (ts, _) in enumerate(segment_files):
        filter_parts.append(f'[{i+1}]adelay={int(ts*1000)}|{int(ts*1000)}[d{i}]')

    mix_inputs = '[0]' + ''.join(f'[d{i}]' for i in range(len(segment_files)))
    filter_parts.append(f'{mix_inputs}amix=inputs={len(segment_files)+1}:normalize=0[out]')

    filter_str = ';'.join(filter_parts)

    synced_path = tmp / f'synced_{lang}.wav'
    cmd = ['ffmpeg', '-y'] + inputs + [
        '-filter_complex', filter_str,
        '-map', '[out]',
        '-ar', '44100', '-ac', '1',
        str(synced_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        log(f"  Sync {lang} FFmpeg erro: {result.stderr[-200:]}")
        return None

    log(f"  Sync {lang}: {synced_path.stat().st_size//1024}KB sincronizado")
    return synced_path

def wait_for_encoding(video_guid, library_id, api_key, timeout_min=30):
    status_names = {0:"na fila",1:"processando",2:"transcoding",
                    3:"redimensionando",4:"concluído",5:"erro",6:"falhou"}
    deadline = time.time() + timeout_min * 60
    while time.time() < deadline:
        try:
            r = requests.get(
                f"https://video.bunnycdn.com/library/{library_id}/videos/{video_guid}",
                headers={"AccessKey": api_key}, timeout=15)
            meta = r.json()
            st  = meta.get("status", 0)
            pct = meta.get("encodeProgress", 0)
            log(f"  Encoding: {status_names.get(st, st)} — {pct}%")
            if st == 4: return True
            if st in (5, 6): return False
        except Exception as e:
            log(f"  Poll warning: {e}")
        time.sleep(15)
    return False

def process_job(job_id, payload):
    try:
        library_id   = payload['library_id']
        api_key      = payload['api_key']
        cdn_host     = payload['cdn_host']
        video_guid   = payload['video_guid']
        video_title  = payload['video_title']
        audio_tracks = payload['audio_tracks']
        srts         = payload.get('srts', {})

        log(f"=== Job {job_id}: {video_title[:50]} ===")
        update_job(job_id, status='running', progress=5, message='Ativando multi-audio no Bunny...')

        enable_multi_audio(library_id, api_key)

        with tempfile.TemporaryDirectory(prefix='bjjprotech_') as tmp:
            tmp = Path(tmp)

            # 1. Download vídeo original
            update_job(job_id, progress=10, message='Baixando vídeo original...')
            video_url = f"https://{cdn_host}/{video_guid}/play_720p.mp4"
            orig_path = tmp / 'original.mp4'
            log(f"  Download: {video_url}")
            download_file(video_url, orig_path)

            # 2. Salvar WAVs
            update_job(job_id, progress=35, message='Preparando faixas de áudio...')
            wav_paths = {}
            for track in audio_tracks:
                lang     = track['lang']
                wav_data = base64.b64decode(track['wav_b64'])
                raw_path = tmp / f'dub_{lang}_raw.bin'
                raw_path.write_bytes(wav_data)
                log(f"  WAV {lang.upper()} raw: {len(wav_data)//1024}KB, magic: {wav_data[:4]}")

                conv_path = tmp / f'dub_{lang}.wav'

                # Try 1: treat as WAV file directly
                conv1 = subprocess.run([
                    'ffmpeg', '-y', '-i', str(raw_path),
                    '-ar', '44100', '-ac', '1', '-c:a', 'pcm_s16le',
                    str(conv_path)
                ], capture_output=True, text=True)

                if conv1.returncode == 0:
                    wav_paths[lang] = conv_path
                    log(f"  WAV {lang.upper()}: {conv_path.stat().st_size//1024}KB (convertido de WAV)")
                else:
                    # Try 2: treat as raw PCM 16-bit 24000Hz (Gemini TTS format)
                    log(f"  Tentando como PCM raw 24kHz...")
                    conv2 = subprocess.run([
                        'ffmpeg', '-y',
                        '-f', 's16le', '-ar', '24000', '-ac', '1',
                        '-i', str(raw_path),
                        '-ar', '44100', '-ac', '1', '-c:a', 'pcm_s16le',
                        str(conv_path)
                    ], capture_output=True, text=True)

                    if conv2.returncode == 0:
                        wav_paths[lang] = conv_path
                        log(f"  WAV {lang.upper()}: {conv_path.stat().st_size//1024}KB (convertido de PCM raw)")
                    else:
                        log(f"  ERRO conversão WAV: {conv2.stderr[-200:]}")
                        raise RuntimeError(f"Não foi possível converter áudio {lang}: {conv2.stderr[-150:]}")

            # 3. FFmpeg
            update_job(job_id, progress=45, message='Mesclando faixas com FFmpeg...')
            merged_path = tmp / 'multilingual.mp4'
            lang_order  = [t['lang'] for t in audio_tracks]
            lang_labels = {t['lang']: t['label'] for t in audio_tracks}
            srts_data   = payload.get('srts_audio', {})  # SRT per lang for sync

            # If SRT data provided, sync each audio track to SRT timestamps
            for lang in lang_order:
                if lang in srts_data and srts_data[lang] and lang in wav_paths:
                    synced = sync_audio_to_srt(wav_paths[lang], srts_data[lang], tmp, lang)
                    if synced:
                        wav_paths[lang] = synced
                        log(f"  Sync SRT {lang.upper()}: OK")

            LANG_ISO = {'pt':'por','en':'eng','es':'spa','fr':'fra'}

            cmd = ['ffmpeg', '-y']
            # Input: vídeo original
            cmd += ['-i', str(orig_path)]
            # Inputs: WAVs dublados (já convertidos para PCM 44100Hz)
            for lang in lang_order:
                cmd += ['-i', str(wav_paths[lang])]

            # Mapear vídeo e todas as faixas de áudio
            cmd += ['-map', '0:v']   # vídeo original — copiado sem alteração
            cmd += ['-map', '0:a']   # áudio PT-BR original — copiado sem alteração

            for i in range(len(lang_order)):
                cmd += ['-map', f'{i+1}:a']

            # Metadados de idioma
            cmd += ['-metadata:s:a:0', 'language=por']
            cmd += ['-metadata:s:a:0', 'title=Português (Brasil)']
            for idx, lang in enumerate(lang_order, 1):
                iso   = LANG_ISO.get(lang, lang)
                label = lang_labels.get(lang, lang.upper())
                cmd += [f'-metadata:s:a:{idx}', f'language={iso}']
                cmd += [f'-metadata:s:a:{idx}', f'title={label}']

            # GARANTIA DE QUALIDADE:
            # -c:v copy   → vídeo copiado bit a bit, zero perda de qualidade
            # -c:a:0 copy → áudio PT-BR original copiado bit a bit, zero alteração
            # Faixas EN/ES convertidas para AAC 128k (qualidade adequada para dublagem)
            cmd += ['-c:v', 'copy']      # vídeo intocado
            cmd += ['-c:a:0', 'copy']    # áudio PT-BR intocado
            for i in range(len(lang_order)):
                cmd += [f'-c:a:{i+1}', 'aac', f'-b:a:{i+1}', '128k']

            cmd.append(str(merged_path))

            log(f"  FFmpeg iniciando...")
            log(f"  CMD: {' '.join(cmd[:12])}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
            if result.returncode != 0:
                # Show last 1000 chars of stderr for debugging
                err_detail = result.stderr[-1000:] if result.stderr else "sem stderr"
                log(f"  FFmpeg STDERR: {err_detail}")
                raise RuntimeError(f"FFmpeg código {result.returncode}: {result.stderr[-300:]}")
            log(f"  FFmpeg OK: {merged_path.stat().st_size//1024//1024}MB")

            # 4. Upload via Bunny Fetch (preserva GUID original)
            update_job(job_id, progress=60, message='Enviando vídeo para Bunny (Fetch API)...')
            new_guid = upload_to_bunny(video_guid, library_id, api_key, merged_path, video_title)

            # 5. Aguardar encoding
            update_job(job_id, progress=75, message='Aguardando re-encoding no Bunny...')
            wait_for_encoding(new_guid, library_id, api_key)

            # 6. Legendas
            update_job(job_id, progress=90, message='Enviando legendas SRT...')
            LANG_LABELS = {'pt':'Português (Brasil)','en':'English (US)',
                           'es':'Español (ES)','fr':'Français (FR)'}
            for lang, srt_content in srts.items():
                if not srt_content.strip(): continue
                ok_cap = upload_caption(new_guid, library_id, api_key,
                                        lang, LANG_LABELS.get(lang, lang), srt_content)
                log(f"  Legenda {lang.upper()}: {'OK' if ok_cap else 'WARN'}")

        update_job(job_id, status='done', progress=100,
                   message=f'Concluído! GUID preservado: {new_guid} — vídeo com multi-audio no Bunny!')
        log(f"=== Job {job_id} CONCLUÍDO ===")

    except Exception as e:
        import traceback
        log(f"=== Job {job_id} ERRO: {e} ===\n{traceback.format_exc()}")
        update_job(job_id, status='error', message=str(e)[:200])

@app.route('/health', methods=['GET'])
def health():
    ffmpeg_ok = subprocess.run(['ffmpeg','-version'], capture_output=True).returncode == 0
    railway_domain = os.environ.get('RAILWAY_PUBLIC_DOMAIN', 'não configurado')
    return jsonify({"status": "ok", "ffmpeg": ffmpeg_ok, "domain": railway_domain})

@app.route('/', methods=['GET'])
@app.route('/app', methods=['GET'])
def serve_app():
    """Serve a aplicação HTML diretamente do Railway."""
    from flask import send_file
    app_path = os.path.join(os.path.dirname(__file__), 'app.html')
    if os.path.exists(app_path):
        return send_file(app_path, mimetype='text/html')
    return "<h2>app.html não encontrado. Faça upload do arquivo app.html no repositório.</h2>", 404

@app.route('/temp/<token>', methods=['GET'])
def serve_temp(token):
    """Serve arquivo temporário para o Bunny Fetch baixar."""
    from flask import send_file, abort
    with TEMP_FILES_LOCK:
        file_path = TEMP_FILES.get(token)
    if not file_path or not os.path.exists(file_path):
        abort(404)
    log(f"  Bunny baixando arquivo temporário: {token[:8]}...")
    return send_file(file_path, mimetype='video/mp4')

@app.route('/job', methods=['POST'])
def create_job():
    payload = request.get_json(force=True)
    if not payload:
        return jsonify({"error": "JSON inválido"}), 400
    required = ['library_id','api_key','cdn_host','video_guid','video_title','audio_tracks']
    missing  = [k for k in required if k not in payload]
    if missing:
        return jsonify({"error": f"Campos faltando: {missing}"}), 400

    job_id = f"job_{int(time.time()*1000)}"
    with JOBS_LOCK:
        JOBS[job_id] = {
            "status": "queued", "progress": 0,
            "message": "Na fila...",
            "video_title": payload['video_title'],
            "video_guid":  payload['video_guid'],
            "created_at":  time.time(),
        }

    threading.Thread(target=process_job, args=(job_id, payload), daemon=True).start()
    log(f"Job criado: {job_id} — {payload['video_title'][:40]}")
    return jsonify({"job_id": job_id, "status": "queued"}), 202

@app.route('/job/<job_id>', methods=['GET'])
def get_job(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Job não encontrado"}), 404
    return jsonify(job)

@app.route('/jobs', methods=['GET'])
def list_jobs():
    with JOBS_LOCK:
        return jsonify(dict(JOBS))

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    log(f"BJJ Pro Tech Server iniciado na porta {port}")
    app.run(host='0.0.0.0', port=port, threaded=True)
