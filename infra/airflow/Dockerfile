FROM apache/airflow:2.9.0
# Pre-install the Docker provider as the airflow user so the container
# does not need _PIP_ADDITIONAL_REQUIREMENTS (which breaks under root).
RUN pip install --no-cache-dir apache-airflow-providers-docker==3.10.0
