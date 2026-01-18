import argparse
import hashlib
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


def _client_from_env():
    minio_port = _env("MINIO_PORT")
    minio_host = f"http://127.0.0.1:{minio_port}" if minio_port else None
    endpoint = _env("S3_ENDPOINT", minio_host)
    access_key = _env("S3_ACCESS_KEY", _env("MINIO_ROOT_USER"))
    secret_key = _env("S3_SECRET_KEY", _env("MINIO_ROOT_PASSWORD"))
    region = _env("S3_REGION", "us-east-1")
    if not endpoint:
        raise SystemExit("S3 endpoint is not set (S3_ENDPOINT or MINIO_PORT).")
    if not access_key or not secret_key:
        raise SystemExit("S3 credentials are not set (S3_ACCESS_KEY/S3_SECRET_KEY or MINIO_ROOT_*).")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_file(path: Path, size_mb: int) -> None:
    chunk = os.urandom(1024 * 1024)
    with path.open("wb") as handle:
        for _ in range(size_mb):
            handle.write(chunk)


def _ensure_bucket(client, bucket: str) -> None:
    buckets = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    if bucket not in buckets:
        client.create_bucket(Bucket=bucket)


def _parallel_head_get(client, bucket: str, key: str, workers: int, rounds: int) -> None:
    def _task():
        head = client.head_object(Bucket=bucket, Key=key)
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        return head.get("ContentLength"), len(body)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_task) for _ in range(rounds)]
        for fut in as_completed(futures):
            content_length, body_len = fut.result()
            if content_length != body_len:
                raise RuntimeError(f"Parallel read mismatch: {content_length=} {body_len=}")


def main() -> None:
    parser = argparse.ArgumentParser(description="S3 gate test for SeaweedFS/MinIO compatibility.")
    parser.add_argument("--bucket", default=_env("S3_BUCKET", "lidar-data"))
    parser.add_argument("--prefix", default="s3-gate")
    parser.add_argument("--small-size-kb", type=int, default=4)
    parser.add_argument("--large-size-mb", type=int, default=64)
    parser.add_argument("--parallel-workers", type=int, default=16)
    parser.add_argument("--parallel-rounds", type=int, default=64)
    parser.add_argument("--multipart-threshold-mb", type=int, default=8)
    args = parser.parse_args()

    client = _client_from_env()
    bucket = args.bucket
    _ensure_bucket(client, bucket)

    stamp = int(time.time())
    prefix = f"{args.prefix}/{stamp}"
    small_key = f"{prefix}/small.bin"
    large_key = f"{prefix}/large.bin"

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        small_path = tmp_path / "small.bin"
        large_path = tmp_path / "large.bin"

        small_path.write_bytes(os.urandom(args.small_size_kb * 1024))
        _write_file(large_path, args.large_size_mb)

        client.put_object(Bucket=bucket, Key=small_key, Body=small_path.read_bytes())
        head = client.head_object(Bucket=bucket, Key=small_key)
        body = client.get_object(Bucket=bucket, Key=small_key)["Body"].read()
        if head.get("ContentLength") != len(body):
            raise RuntimeError("Small object content length mismatch.")
        if body != small_path.read_bytes():
            raise RuntimeError("Small object hash mismatch.")

        transfer_config = TransferConfig(multipart_threshold=args.multipart_threshold_mb * 1024 * 1024)
        client.upload_file(str(large_path), bucket, large_key, Config=transfer_config)
        download_path = tmp_path / "large.download.bin"
        client.download_file(bucket, large_key, str(download_path))
        if _sha256(large_path) != _sha256(download_path):
            raise RuntimeError("Large object hash mismatch after multipart upload/download.")

        _parallel_head_get(client, bucket, small_key, args.parallel_workers, args.parallel_rounds)

    client.delete_object(Bucket=bucket, Key=small_key)
    client.delete_object(Bucket=bucket, Key=large_key)
    print("S3 gate tests passed.")


if __name__ == "__main__":
    main()
