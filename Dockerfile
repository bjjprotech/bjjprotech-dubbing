FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir flask requests

WORKDIR /app
COPY server.py .

EXPOSE 8080

CMD ["python", "server.py"]
