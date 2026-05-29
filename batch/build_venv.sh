#!/bin/bash
set -e
echo "Building venv for Spark inside container..."
apt-get update && apt-get install -y python3-venv libgl1-mesa-glx libglib2.0-0
pip3 install -r /opt/spark/batch/requirements.txt
python3 -m venv /tmp/.venv_spark
source /tmp/.venv_spark/bin/activate
pip install --upgrade pip
pip install -r /opt/spark/batch/requirements.txt
# remove opencv-python-headless to avoid conflicts
pip uninstall -y opencv-python-headless || true
venv-pack -o /opt/spark/batch/environment.tar.gz -f
chmod 644 /opt/spark/batch/environment.tar.gz
echo "Done!"
