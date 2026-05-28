#!/bin/bash
set -e
echo "Building venv for Spark inside container..."
apt-get update && apt-get install -y python3-venv libgl1-mesa-glx libglib2.0-0
pip3 install sqlalchemy psycopg2-binary
python3 -m venv /tmp/.venv_spark
source /tmp/.venv_spark/bin/activate
pip install --upgrade pip
pip install minio scenedetect[opencv] av numpy venv-pack clickhouse-connect==0.7.19
# remove opencv-python-headless to avoid conflicts
pip uninstall -y opencv-python-headless || true
venv-pack -o /opt/spark/batch/environment.tar.gz -f
chmod 644 /opt/spark/batch/environment.tar.gz
echo "Done!"
