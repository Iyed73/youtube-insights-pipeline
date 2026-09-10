FROM apache/spark:4.0.1-python3

USER root

COPY batch/requirements.txt /tmp/batch-requirements.txt
RUN pip install --no-cache-dir -r /tmp/batch-requirements.txt \
    && rm /tmp/batch-requirements.txt

# Models are baked in so jobs never download at runtime. The spark user has no
# home directory, so the caches live under /opt/spark.
ARG WHISPER_MODEL=base
ENV WHISPER_MODEL=${WHISPER_MODEL} \
    HF_HOME=/opt/spark/hf_cache \
    NLTK_DATA=/opt/spark/nltk_data
RUN python3 -c "from faster_whisper import download_model; download_model('${WHISPER_MODEL}')" \
    && python3 -m nltk.downloader -d "${NLTK_DATA}" wordnet \
    && chmod -R a+rX "${HF_HOME}" "${NLTK_DATA}"

COPY batch/src /opt/spark/work/batch
ENV PYTHONPATH=/opt/spark/work/batch

USER spark
