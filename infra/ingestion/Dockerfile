FROM python:3.11-slim

# ffmpeg is required by yt-dlp to merge video+audio streams.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Preserve the repo's relative directory structure so that
#   Path(__file__).parents[3]          →  /app
#   .../producers/base.py  parents[3]  →  /app  → /app/schemas/
#   .../channel_discovery/main.py p[3] →  /app  → /app/config/channels.yaml
COPY ingestion/ /app/ingestion/
COPY schemas/   /app/schemas/
COPY config/    /app/config/

RUN pip install --no-cache-dir /app/ingestion/

# Modules live directly in src/ and are not installed packages.
ENV PYTHONPATH=/app/ingestion/src
WORKDIR /app/ingestion/src
