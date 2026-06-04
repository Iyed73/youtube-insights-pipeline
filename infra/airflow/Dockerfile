FROM apache/airflow:2.9.0
# Pinning apache-airflow keeps pip from upgrading Airflow while resolving the provider.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-docker==3.10.0
