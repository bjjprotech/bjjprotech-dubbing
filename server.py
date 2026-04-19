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

def upload_to_bunny(video_guid, library_id, api_key, file_path):
    url = f"https://video.bunnycdn.com/library/{library_id}/videos/{video_guid}"
    headers = {"AccessKey": api_key, "Content-Type": "application/octet-stream"}
    file_size = os.path.getsize(file_path)
    log(f"  Upload: {file_size//1024//1024}MB → Bunny ID: {video_guid[:8]}...")
    with open(file_path, 'rb') as f:
        r = requests.put(url, headers=headers, data=f, timeout=3600)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Upload falhou: HTTP {r.status_code} — {r.text[:200]}")
    log(f"  Upload OK — ID preservado: {video_guid}")

def upload_caption(video_guid, library_id, api_key, lang, label, srt_content):
    b64 = base64.b64encode(srt_content.encode('utf-8')).decode('ascii')
    url = f"https://video.bunnycdn.com/library/{library_id}/videos/{video_guid}/captions/{lang}"
    r = requests.post(url,
        headers={"AccessKey": api_key, "Content-Type": "application/json"},
        json={"srclang": lang, "label": label, "captionsFile": b64},
        timeout=30)
    return r.status_code in (200, 201)

def enable_multi_audio(library_id, api_key):
    try:
        r = requests.post(
            f"https://api.bunny.net/videolibrary/{library_id}",
            headers={"AccessKey": api_key, "Content-Type": "application/json"},
            json={"EnableMultiAudioTrackSupport": True}, timeout=15)
        log(f"  Multi Audio Track: HTTP {r.status_code}")
    except Exception as e:
        log(f"  Multi Audio Track warning: {e}")

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

            cmd = ['ffmpeg', '-y', '-i', str(orig_path)]
            for lang in lang_order:
                cmd += ['-i', str(wav_paths[lang])]
            cmd += ['-map', '0:v', '-map', '0:a']
            for i in range(len(lang_order)):
                cmd += ['-map', f'{i+1}:a']

            LANG_ISO = {'pt':'por','en':'eng','es':'spa','fr':'fra'}
            cmd += ['-metadata:s:a:0','language=por','-metadata:s:a:0','title=Português']
            for idx, lang in enumerate(lang_order, 1):
                iso   = LANG_ISO.get(lang, lang)
                label = lang_labels.get(lang, lang.upper())
                cmd += [f'-metadata:s:a:{idx}', f'language={iso}']
                cmd += [f'-metadata:s:a:{idx}', f'title={label}']

            cmd += ['-c:v','copy','-c:a:0','copy']
            for i in range(len(lang_order)):
                cmd += [f'-c:a:{i+1}','aac',f'-b:a:{i+1}','128k']
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

            # 4. Upload
            update_job(job_id, progress=60, message='Fazendo re-upload no Bunny...')
            upload_to_bunny(video_guid, library_id, api_key, merged_path)

            # 5. Aguardar encoding
            update_job(job_id, progress=75, message='Aguardando re-encoding no Bunny...')
            wait_for_encoding(video_guid, library_id, api_key)

            # 6. Legendas
            update_job(job_id, progress=90, message='Enviando legendas SRT...')
            LANG_LABELS = {'pt':'Português (Brasil)','en':'English (US)',
                           'es':'Español (ES)','fr':'Français (FR)'}
            for lang, srt_content in srts.items():
                if not srt_content.strip(): continue
                ok_cap = upload_caption(video_guid, library_id, api_key,
                                        lang, LANG_LABELS.get(lang, lang), srt_content)
                log(f"  Legenda {lang.upper()}: {'OK' if ok_cap else 'WARN'}")

        update_job(job_id, status='done', progress=100,
                   message=f'Concluído! Vídeo {video_guid[:8]} atualizado no Bunny.')
        log(f"=== Job {job_id} CONCLUÍDO ===")

    except Exception as e:
        import traceback
        log(f"=== Job {job_id} ERRO: {e} ===\n{traceback.format_exc()}")
        update_job(job_id, status='error', message=str(e)[:200])

@app.route('/health', methods=['GET'])
def health():
    ffmpeg_ok = subprocess.run(['ffmpeg','-version'], capture_output=True).returncode == 0
    return jsonify({"status": "ok", "ffmpeg": ffmpeg_ok})

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
