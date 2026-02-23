from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from legacy_env_vars import settings
from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Ref, S3Store, raw_cloud_key, raw_control_point_key, raw_path_key, scan_prefix


@dataclass
class ArtifactService:
    repo: Repo
    s3: S3Store
    bucket: str

    def upload_raw_artifact(
        self,
        *,
        company_id: str,
        dataset_version_id: str,
        scan_id: str,
        artifact_kind: str,
        local_file_path: str,
        filename: str | None = None,
    ) -> dict[str, Any]:
        scan = self.repo.get_scan(scan_id)
        if scan.company_id != company_id or scan.dataset_version_id != dataset_version_id:
            raise RuntimeError(
                f"Scan {scan_id} does not belong to company {company_id}/dataset_version {dataset_version_id}"
            )

        prefix = scan_prefix(company_id, dataset_version_id, scan_id)
        artifact_filename = filename if filename is not None else Path(local_file_path).name

        if artifact_kind == "raw.point_cloud":
            key = raw_cloud_key(prefix, artifact_filename)
        elif artifact_kind == "raw.trajectory":
            key = raw_path_key(prefix)
        elif artifact_kind == "raw.control_point":
            key = raw_control_point_key(prefix)
        else:
            raise ValueError(f"Unknown artifact kind: {artifact_kind}")

        ref = S3Ref(self.bucket, key)
        etag, size = self.s3.put_object(ref, local_file_path)

        self.repo.register_raw_artifact(
            company_id=company_id,
            scan_id=scan_id,
            kind=artifact_kind,
            bucket=ref.bucket,
            key=ref.key,
            etag=etag,
            size_bytes=size,
            meta={"filename": artifact_filename},
        )

        return {
            "bucket": ref.bucket,
            "key": ref.key,
            "etag": etag,
            "size_bytes": size,
            "kind": artifact_kind,
        }

    def upload_derived_bytes(
        self,
        *,
        company_id: str,
        scan_id: str,
        schema_version: str,
        kind: str,
        key: str,
        data: bytes,
        content_type: str,
        status: str = "AVAILABLE",
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        etag, size = self.s3.put_bytes(S3Ref(self.bucket, key), data, content_type=content_type)
        self.repo.register_artifact(
            company_id=company_id,
            scan_id=scan_id,
            kind=kind,
            bucket=self.bucket,
            key=key,
            schema_version=schema_version,
            etag=etag,
            size_bytes=size,
            status=status,
            meta=meta or {},
        )
        return {
            "bucket": self.bucket,
            "key": key,
            "etag": etag,
            "size_bytes": size,
            "kind": kind,
        }

    def upload_derived_file(
        self,
        *,
        company_id: str,
        scan_id: str,
        schema_version: str,
        kind: str,
        key: str,
        local_file_path: str,
        status: str = "AVAILABLE",
        meta: dict[str, Any] | None = None,
        use_upload_file: bool = False,
    ) -> dict[str, Any]:
        ref = S3Ref(self.bucket, key)
        if use_upload_file:
            etag, size = self.s3.upload_file(ref, local_file_path)
        else:
            etag, size = self.s3.put_object(ref, local_file_path)

        self.repo.register_artifact(
            company_id=company_id,
            scan_id=scan_id,
            kind=kind,
            bucket=ref.bucket,
            key=ref.key,
            schema_version=schema_version,
            etag=etag,
            size_bytes=size,
            status=status,
            meta=meta or {},
        )

        return {
            "bucket": ref.bucket,
            "key": ref.key,
            "etag": etag,
            "size_bytes": size,
            "kind": kind,
        }

    def upsert_derived_file(
        self,
        *,
        company_id: str,
        scan_id: str,
        schema_version: str,
        kind: str,
        key: str,
        local_file_path: str,
        status: str = "READY",
        meta: dict[str, Any] | None = None,
        use_upload_file: bool = False,
    ) -> dict[str, Any]:
        ref = S3Ref(self.bucket, key)
        if use_upload_file:
            etag, size = self.s3.upload_file(ref, local_file_path)
        else:
            etag, size = self.s3.put_object(ref, local_file_path)

        self.repo.upsert_derived_artifact(
            company_id=company_id,
            scan_id=scan_id,
            kind=kind,
            schema_version=schema_version,
            s3_bucket=ref.bucket,
            s3_key=ref.key,
            etag=etag,
            size_bytes=size,
            status=status,
            meta=meta or {},
        )

        return {
            "bucket": ref.bucket,
            "key": ref.key,
            "etag": etag,
            "size_bytes": size,
            "kind": kind,
        }


def build_artifact_service() -> ArtifactService:
    s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)
    repo = Repo()
    return ArtifactService(repo=repo, s3=s3, bucket=settings.s3_bucket)


def store_artifact(
    *,
    repo: Repo,
    s3: S3Store,
    company_id: str,
    scan_id: str,
    kind: str,
    bucket: str,
    dataset_version_id: str | None = None,
    filename: str | None = None,
    key: str | None = None,
    schema_version: str | None = None,
    local_file_path: str | None = None,
    data: bytes | None = None,
    content_type: str = "application/octet-stream",
    status: str = "AVAILABLE",
    meta: dict[str, Any] | None = None,
    upsert: bool = False,
    upload_method: str = "put_object",
) -> dict[str, Any]:
    service = ArtifactService(repo=repo, s3=s3, bucket=bucket)

    if kind.startswith("raw."):
        if dataset_version_id is None:
            raise ValueError("dataset_version_id is required for raw artifacts")
        if local_file_path is None:
            raise ValueError("local_file_path is required for raw artifacts")
        return service.upload_raw_artifact(
            company_id=company_id,
            dataset_version_id=dataset_version_id,
            scan_id=scan_id,
            artifact_kind=kind,
            local_file_path=local_file_path,
            filename=filename,
        )

    if schema_version is None:
        raise ValueError("schema_version is required for derived artifacts")
    if key is None:
        raise ValueError("key is required for derived artifacts")

    if data is not None:
        return service.upload_derived_bytes(
            company_id=company_id,
            scan_id=scan_id,
            schema_version=schema_version,
            kind=kind,
            key=key,
            data=data,
            content_type=content_type,
            status=status,
            meta=meta,
        )

    if local_file_path is None:
        raise ValueError("local_file_path is required when data is not provided")

    use_upload_file = upload_method == "upload_file"
    if upsert:
        return service.upsert_derived_file(
            company_id=company_id,
            scan_id=scan_id,
            schema_version=schema_version,
            kind=kind,
            key=key,
            local_file_path=local_file_path,
            status=status,
            meta=meta,
            use_upload_file=use_upload_file,
        )

    return service.upload_derived_file(
        company_id=company_id,
        scan_id=scan_id,
        schema_version=schema_version,
        kind=kind,
        key=key,
        local_file_path=local_file_path,
        status=status,
        meta=meta,
        use_upload_file=use_upload_file,
    )


def download_artifact(*, s3: S3Store, bucket: str, key: str, dst_dir: Path) -> Path:
    dst = dst_dir / Path(key).name
    s3.download_file(S3Ref(bucket, key), str(dst))
    if not dst.exists():
        raise RuntimeError(f"downloaded file missing: s3://{bucket}/{key} -> {dst}")
    return dst
