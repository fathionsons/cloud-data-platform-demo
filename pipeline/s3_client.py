from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.client import BaseClient


class S3Client:
    def __init__(self, *, region_name: str) -> None:
        self._client: BaseClient = boto3.client("s3", region_name=region_name)

    def put_json_lines(self, *, bucket: str, key: str, rows: list[dict[str, Any]]) -> None:
        payload = "\n".join(json.dumps(row, default=str) for row in rows)
        self._client.put_object(Bucket=bucket, Key=key, Body=payload.encode("utf-8"))
