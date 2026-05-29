FROM apache/spark:4.0.1-python3

USER root
COPY batch/pyproject.toml /tmp/batch/pyproject.toml
RUN sed -n '/^dependencies/,/]/p' /tmp/batch/pyproject.toml \
    | grep '"' | sed 's/.*"\(.*\)".*/\1/' > /tmp/requirements.txt \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip list \
    && rm -rf /tmp/batch /tmp/requirements.txt
# Pre-download the Whisper model so it's baked into the image.
# Use a shared cache dir that the spark user can read at runtime.
ENV HF_HOME=/opt/spark/hf_cache
RUN python3 -c "from faster_whisper import WhisperModel; WhisperModel('base', device='cpu', compute_type='int8')" \
    && chmod -R a+rX /opt/spark/hf_cache

# Bake the batch source code into the image so DockerOperator containers
# (which have no volume mounts) can find the job files.
# spark-master and spark-worker override this at runtime via their bind mount.
COPY batch/src /opt/spark/work/batch

USER spark
