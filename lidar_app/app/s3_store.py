import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

import re
_ALLOWED = re.compile(r"[^a-zA-Z0-9._-]+")
import unicodedata

from dataclasses import dataclass
from typing import Tuple
from pathlib import Path


@dataclass(frozen=True)
class S3Ref:
    bucket: str
    key: str


class S3Store:
    def __init__(self, endpoint, access_key, secret_key, region):
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(signature_version='s3v4',
                          s3={'addressing_style': 'path'},
            proxies={}
            ),
        )

    def upload_file(self, ref: S3Ref, local_path: str) -> Tuple[str, int]:
        self.client.upload_file(local_path, ref.bucket, ref.key)
        head = self.client.head_object(Bucket=ref.bucket, Key=ref.key)
        etag = head['ETag'].strip('"')
        size = int(head['ContentLength'])
        return etag, size

    def get_bytes(self, ref: S3Ref) -> bytes:
        resp = self.client.get_object(Bucket=ref.bucket, Key=ref.key)
        return resp['Body'].read()

    def download_file(self, ref: S3Ref, local_path: str) -> None:
        local_path = str(Path(local_path))
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(ref.bucket, ref.key, local_path)

    def put_object(self, ref: S3Ref, local_path: str) -> Tuple[str, int]:
        local_path = str(Path(local_path))
        with open(local_path, 'rb') as f:
            self.client.put_object(Bucket=ref.bucket, Key=ref.key, Body=f)
        head = self.client.head_object(Bucket=ref.bucket, Key=ref.key)
        etag = head['ETag'].strip('"')
        size = int(head['ContentLength'])
        return etag, size

    def put_bytes(
            self,
            ref: S3Ref,
            data: bytes,
            *,
            content_type: str = 'application/octet-stream',
    ) -> Tuple[str | None, int | None]:
        resp = self.client.put_object(
            Bucket=ref.bucket,
            Key=ref.key,
            Body=data,
            ContentType=content_type,
        )
        etag = resp.get('ETag')
        return etag.strip('"') if isinstance(etag, str) else None, len(data)

    def head_object(self, ref: S3Ref) -> Tuple[str | None, int | None]:
        try:
            head = self.client.head_object(Bucket=ref.bucket, Key=ref.key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None, None
            raise
        etag = head.get("ETag")
        size = head.get("ContentLength")
        return etag.strip('"') if isinstance(etag, str) else None, int(size) if size is not None else None


def derived_manifest_key(prefix: str, schema_version: str) -> str:
    prefix = prefix.rstrip("/")
    return f"{prefix}/derived/v{schema_version}/ingest_manifest.json"

def safe_segment(s: str) -> str:
    s = unicodedata.normalize('NFKC', s).strip()
    s = _ALLOWED.sub('_', s)
    return s.strip('_') or 'na'

def scan_prefix(company_id: str, dataset_version_id: str, scan_id: str) -> str:
    cid = safe_segment(company_id)          # можно оставить, пока company_id не ULID
    dvid = safe_segment(dataset_version_id)
    sid = safe_segment(scan_id)
    return f"tenants/{cid}/dataset_versions/{dvid}/scans/{sid}"

def raw_cloud_key(prefix: str, filename: str) -> str:
    prefix = prefix.rstrip("/")
    return f"{prefix}/raw/point_cloud/{filename}"

def raw_path_key(prefix: str) -> str:
    prefix = prefix.rstrip("/")
    return f"{prefix}/raw/trajectory/path.txt"

def raw_control_point_key(prefix: str) -> str:
    prefix = prefix.rstrip("/")
    return f"{prefix}/raw/control_points/ControlPoint.txt"
