# pyright: ignore-file

"""Cloudflare R2 storage helper for the R2 suite: the object operations the tests run at the source.

Self-contained on purpose, so the R2 suite does not move when another connector's suite does.
"""

from __future__ import annotations

from pathlib import Path

import boto3
from botocore.config import Config
from helper.run_folder import require_run_folder


def r2_endpoint(account_id: str, jurisdiction: str | None = None) -> str:
    sub = f".{jurisdiction}" if jurisdiction else ""
    return f"https://{account_id}{sub}.r2.cloudflarestorage.com"


class R2StorageHelper:
    """boto3 pointed at one R2 account, signing for region ``auto``."""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str) -> None:
        self._client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            region_name="auto",
            # botocore's default checksum headers are not in R2's list of supported ones.
            config=Config(request_checksum_calculation="when_required", response_checksum_validation="when_required"),
        )

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        keys: list[str] = []
        for page in self._client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents") or [] if obj.get("Key"))
        return keys

    def upload_directory(self, bucket: str, root: Path, prefix: str = "") -> int:
        root = root.resolve()
        files = [path for path in root.rglob("*") if path.is_file()]
        for path in files:
            self._client.upload_file(str(path), bucket, prefix + path.relative_to(root).as_posix())
        return len(files)

    def upload_object(self, bucket: str, key: str, data: bytes, content_type: str | None = None) -> None:
        extra = {"ContentType": content_type} if content_type else {}
        self._client.put_object(Bucket=bucket, Key=key, Body=data, **extra)

    def overwrite_object(self, bucket: str, key: str, data: bytes, content_type: str | None = None) -> None:
        self.upload_object(bucket, key, data, content_type)

    def rename_object(self, bucket: str, old_key: str, new_key: str) -> None:
        # No rename in the S3 API: copy then delete. The copy keeps the ETag, which is how
        # the connector recognises the move and keeps the same record.
        self._client.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": old_key}, Key=new_key)
        self._client.delete_object(Bucket=bucket, Key=old_key)

    def delete_object(self, bucket: str, key: str) -> None:
        self._client.delete_object(Bucket=bucket, Key=key)

    def clear_objects(self, bucket: str, prefix: str) -> None:
        """Delete everything under this run's folder, and nothing else. R2 keeps no object versions."""
        prefix = require_run_folder(prefix)
        for page in self._client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            contents = page.get("Contents") or []
            if contents:
                self._client.delete_objects(
                    Bucket=bucket, Delete={"Objects": [{"Key": obj["Key"]} for obj in contents]}
                )
