from __future__ import annotations

import io

from minio import Minio
from minio.error import S3Error

from utils.config import MinioConfig


class ObjectStore:
    def __init__(self, cfg: MinioConfig) -> None:
        self._client = Minio(
            cfg.endpoint, access_key=cfg.access_key, secret_key=cfg.secret_key, secure=False
        )
        self._bucket = cfg.bucket

    def exists(self, key: str) -> bool:
        try:
            self._client.stat_object(self._bucket, key)
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return False
            raise
        return True

    def read(self, key: str) -> bytes | None:
        try:
            response = self._client.get_object(self._bucket, key)
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return None
            raise
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def write(self, key: str, data: bytes, content_type: str) -> None:
        self._client.put_object(
            self._bucket, key, io.BytesIO(data), length=len(data), content_type=content_type
        )

    def download(self, key: str, path: str) -> None:
        self._client.fget_object(self._bucket, key, path)
