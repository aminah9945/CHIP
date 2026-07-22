from __future__ import annotations

import io
import json
from typing import Any
from minio import Minio


class BronzeClient:
    """MinIO Bronze layer archival client.

    Archives raw content and sidecar metadata to MinIO using content-addressed,
    immutable paths.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str = "chip-bronze",
        secure: bool = False,
        client: Minio | None = None,
    ) -> None:
        self.bucket = bucket
        self.client = client or Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def _ensure_bucket(self) -> None:
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def archive(
        self,
        source: str,
        identity: str,
        content: bytes,
        content_type: str,
        content_hash: str,
        original_filename: str,
        metadata: dict[str, Any],
    ) -> str:
        """Archive raw content and sidecar metadata to MinIO.

        Returns the full bronze_uri (e.g. s3://chip-bronze/<source>/<identity>/<hash_slug>/<filename>)
        """
        self._ensure_bucket()

        # Format URL-safe hash slug (replace sha256: with sha256-)
        hash_slug = content_hash.replace(":", "-")

        # Object paths inside bucket
        object_key = f"{source}/{identity}/{hash_slug}/{original_filename}"
        meta_key = f"{source}/{identity}/{hash_slug}/.meta.json"

        # 1. Upload raw content bytes
        content_stream = io.BytesIO(content)
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=object_key,
            data=content_stream,
            length=len(content),
            content_type=content_type,
        )

        # 2. Upload sidecar .meta.json
        meta_payload = {
            "source": source,
            "identity": identity,
            "content_hash": content_hash,
            "original_filename": original_filename,
            "content_type": content_type,
            "file_size_bytes": len(content),
            **metadata,
        }
        meta_bytes = json.dumps(meta_payload, indent=2).encode("utf-8")
        meta_stream = io.BytesIO(meta_bytes)
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=meta_key,
            data=meta_stream,
            length=len(meta_bytes),
            content_type="application/json",
        )

        return f"s3://{self.bucket}/{object_key}"

    def get(self, bronze_uri: str) -> bytes:
        """Retrieve raw content bytes from MinIO given a bronze_uri."""
        if not bronze_uri.startswith(f"s3://{self.bucket}/"):
            raise ValueError(f"Invalid bronze_uri for bucket {self.bucket}: {bronze_uri}")

        object_key = bronze_uri.replace(f"s3://{self.bucket}/", "", 1)
        response = None
        try:
            response = self.client.get_object(self.bucket, object_key)
            return response.read()
        finally:
            if response is not None:
                response.close()
                response.release_conn()
