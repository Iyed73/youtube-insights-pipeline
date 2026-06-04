FROM python:3.11-slim

# ffmpeg is required by yt-dlp to merge video+audio streams.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# The code finds schemas/ and config/ via Path(__file__).parents[3], so keep the repo layout.
COPY ingestion/ /app/ingestion/
COPY schemas/   /app/schemas/
COPY config/    /app/config/

RUN pip install --no-cache-dir /app/ingestion/

ENV PYTHONPATH=/app/ingestion/src
WORKDIR /app/ingestion/src
