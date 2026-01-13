from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Iterator, cast

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy import select, update, func, case
from sqlalchemy.dialects.postgresql import insert as pg_insert
from ulid import ULID

import hashlib
import json
from datetime import datetime, timezone

from lidar_app.app.db import get_session
from lidar_app.app.models import CRS, Company, Dataset, Scan, Artifact, IngestRun, DatasetVersion, ScanEdge, ScanPose

DEFAULT_SCHEMA_VERSION = "1.1.0"

@dataclass(frozen=True)
class IngestRawResult:
    scan_id: str
    cloud_key: str
    path_key: str
    cp_key: str

class Repo:

    @staticmethod
    @contextmanager
    def session() -> Iterator[Session]:
        db = get_session()
        try:
            yield db
            db.commit()
        except:
            db.rollback()
            raise
        finally:
            db.close()
    # === “сайт” ===

    def ensure_company(self, company_id: str, name: str | None = None) -> None:
        with self.session() as db:
            if db.get(Company, company_id) is not None:
                return
            db.add(Company(id=company_id, name=name or company_id))

    def ensure_dataset(self, company_id: str, *, name: str, crs_id: str | None) -> str:
        with self.session() as db:
            existing = db.execute(
                select(Dataset).where(Dataset.company_id == company_id, Dataset.name == name)
            ).scalar_one_or_none()

            if existing is not None:
                if crs_id is not None and existing.crs_id != crs_id:
                    raise RuntimeError(f"Dataset '{name}' has crs {existing.crs_id}, not {crs_id}")
                return existing.id

            if not db.get(Company, company_id):
                raise RuntimeError(f"Company {company_id} not found")

            if crs_id is not None and not db.get(CRS, crs_id):
                raise RuntimeError(f"CRS {crs_id} not found")

            dataset_id = str(ULID())
            db.add(Dataset(id=dataset_id, company_id=company_id, name=name, crs_id=crs_id))

            try:
                db.flush()  # важно: поймать уникальность тут, а не на commit в __exit__
                return dataset_id
            except IntegrityError:
                db.rollback()
                # кто-то другой успел создать — перечитаем и вернём его id
                existing = db.execute(
                    select(Dataset).where(Dataset.company_id == company_id, Dataset.name == name)
                ).scalar_one()
                if crs_id is not None and existing.crs_id != crs_id:
                    raise RuntimeError(f"Dataset '{name}' has crs {existing.crs_id}, not {crs_id}")
                return existing.id

    def get_active_dataset_version(self, dataset_id: str) -> Optional[DatasetVersion]:
        with self.session() as db:
            dv = db.execute(
                select(DatasetVersion)
                .where(DatasetVersion.dataset_id == dataset_id, DatasetVersion.is_active.is_(True))
            ).scalar_one_or_none()
            if dv:
                db.expunge(dv)
            return dv

    def ensure_dataset_version(self, dataset_id: str) -> DatasetVersion:
        with self.session() as db:
            active = db.execute(
                select(DatasetVersion)
                .where(DatasetVersion.dataset_id == dataset_id, DatasetVersion.is_active.is_(True))
            ).scalar_one_or_none()

            if active is not None:
                db.expunge(active)
                return active

            # первая версия
            dv = DatasetVersion(id=str(ULID()), dataset_id=dataset_id, version=1, is_active=True)
            db.add(dv)
            # чтобы вернуть уже с заполненными полями (в т.ч. server_default)
            db.flush()
            db.expunge(dv)
            return dv

    def bump_dataset_version(self, dataset_id: str) -> DatasetVersion:
        with self.session() as db:
            # блокируем активную, чтобы избежать гонок
            active = db.execute(
                select(DatasetVersion)
                .where(DatasetVersion.dataset_id == dataset_id, DatasetVersion.is_active.is_(True))
                .with_for_update()
            ).scalar_one_or_none()

            if active is None:
                new_version = 1
            else:
                active.is_active = False
                new_version = active.version + 1

            dv = DatasetVersion(id=str(ULID()), dataset_id=dataset_id, version=new_version, is_active=True)
            db.add(dv)
            db.flush()
            db.expunge(dv)
            return dv

    def ensure_crs(
            self,
            crs_id: str,
            *,
            name: str,
            zone_degree: int,
            epsg: int | None = None,
            units: str = "m",
            axis_order: str = "x_east,y_north,z_up",
            meta: dict | None = None,
    ) -> None:
        with self.session() as db:
            if db.get(CRS, crs_id) is not None:
                return

            db.add(CRS(
                id=crs_id,
                name=name,
                zone_degree=zone_degree,
                epsg=epsg,
                units=units,
                axis_order=axis_order,
                meta=meta or {},)
            )

    def get_crs(self, crs_id: str):
        with self.session() as db:
            crs = db.get(CRS, crs_id)
            if not crs:
                raise RuntimeError(f"CRS {crs_id} not found")
            return crs

    def resolve_crs_to_pdal_srs(self, crs_id: str) -> str:
        """
        Return PDAL/PROJ-compatible SRS string.
        Preference:
          1) EPSG:<code> if epsg present
          2) meta.projjson or meta.wkt if you store it
          3) fallback to crs_id (but usually should not happen)
        """
        with self.session() as db:
            row = db.execute(
                select(CRS.epsg, CRS.meta, CRS.id).where(CRS.id == crs_id)
            ).one_or_none()

            if row is None:
                raise RuntimeError(f"CRS {crs_id} not found")

            epsg, meta, _id = row
            meta = meta or {}

            if epsg:
                return f"EPSG:{int(epsg)}"

            # если ты хранишь какие-то определения СК в meta — используй их
            projjson = meta.get("projjson")
            if projjson:
                # PDAL filter reprojection принимает JSON как строку тоже, но лучше WKT/epsg
                return projjson

            wkt = meta.get("wkt")
            if wkt:
                return wkt

            # крайний fallback (скорее всего бесполезно для PDAL)
            return str(_id)

    def create_dataset(self, company_id: str, dataset_id: str,crs_id: str, name: str | None = None):
        with self.session() as db:
            if not db.get(Company, company_id):
                raise RuntimeError(f"Company {company_id} not found")
            if db.get(Dataset, dataset_id):
                raise RuntimeError(f"Dataset {dataset_id} already exists")
            if not db.get(CRS, crs_id):
                raise RuntimeError(f"CRS {crs_id} not found")

            db.add(
                Dataset(
                    id=dataset_id,
                    company_id=company_id,
                    name=name or dataset_id,
                    crs_id=crs_id,
                )
            )

    def create_scan(self, company_id: str, dataset_version_id: str) -> str:
        scan_id = str(ULID())

        with self.session() as db:
            dv = db.get(DatasetVersion, dataset_version_id)
            if dv is None:
                raise RuntimeError(f"DatasetVersion {dataset_version_id} not found")

            ds = db.get(Dataset, dv.dataset_id)
            if ds is None:
                raise RuntimeError(f"Dataset {dv.dataset_id} not found")

            if ds.company_id != company_id:
                raise RuntimeError(f"DatasetVersion {dataset_version_id} does not belong to company {company_id}")

            scan = Scan(
                id=scan_id,
                company_id=company_id,
                dataset_id=ds.id,
                dataset_version_id=dv.id,
                crs_id=ds.crs_id,
                status="CREATED",
                schema_version="1.1.0",
                meta={},
            )
            db.add(scan)

        return scan_id

    def get_scan(self, scan_id: str):
        with self.session() as db:
            scan = db.get(Scan, scan_id)
            if not scan:
                raise RuntimeError(f"Scan {scan_id} not found")
            db.expunge(scan)
            return scan

# Artifacts

    def register_raw_artifact(
            self,
            *,
            company_id: str,
            scan_id: str,
            kind: str,
            bucket: str,
            key: str,
            etag: str | None,
            size_bytes: int | None,
            meta: dict | None = None,
            status: str = 'AVAILABLE',
    ) -> None:
        with self.session() as db:
            scan = db.get(Scan, scan_id)
            if not scan:
                raise RuntimeError(f"Scan {scan_id} not found")
            if scan.company_id != company_id:
                raise RuntimeError(f"Scan {scan_id} does not belong to company {company_id}")

            art = Artifact(
                    company_id=company_id,
                    scan_id=scan_id,
                    kind=kind,
                    schema_version=None,  # raw
                    s3_bucket=bucket,
                    s3_key=key,
                    etag=etag,
                    size_bytes=size_bytes,
                    status=status,
                    meta=meta or {},
                )
            db.add(art)

    def register_artifact(
        self,
        company_id: str,
        scan_id: str,
        kind: str,
        bucket: str,
        key: str,
        *,
        schema_version: str,
        etag: str | None,
        size_bytes: int | None,
        meta: dict | None = None,
        status: str = "AVAILABLE",
    ) -> None:
        if not schema_version:
            raise ValueError("Schema version must be provided for derived artifacts")
        with self.session() as db:
            scan = db.get(Scan, scan_id)
            if not scan:
                raise RuntimeError(f"Scan {scan_id} not found")
            if scan.company_id != company_id:
                raise RuntimeError(f"Scan {scan_id} does not belong to company {company_id}")

            art = Artifact(
                company_id=company_id,
                scan_id=scan_id,
                kind=kind,
                schema_version=schema_version,
                s3_bucket=bucket,
                s3_key=key,
                etag=etag,
                size_bytes=size_bytes,
                status=status,
                meta=meta or {},
            )
            db.add(art)

    # === для ingest ===

    def get_scan_bundle(self, scan_id: str) -> dict:
        with self.session() as db:
            scan = db.get(Scan, scan_id)
            if not scan:
                raise RuntimeError(f"Scan {scan_id} not found")

            # Raw artifacts are schema_version IS NULL
            arts = (
                db.execute(
                    select(Artifact).where(
                        Artifact.scan_id == scan_id,
                        Artifact.schema_version.is_(None),
                    )
                )
                .scalars()
                .all()
            )

            def find(kind: str):
                return next((a for a in arts if a.kind == kind), None)

            bundle = {
                "scan": scan,
                "raw_point_cloud": find("raw.point_cloud"),
                "raw_trajectory": find("raw.trajectory"),
                "raw_control_point": find("raw.control_point"),
            }

            for v in bundle.values():
                if v is not None:
                    db.expunge(v)

            return bundle

    @staticmethod
    def _fingerprint_raw_inputs(arts: list[Artifact]) -> str:
        items = []
        for a in arts:
            items.append({
                'kind': a.kind,
                'bucket': a.s3_bucket,
                'key': a.s3_key,
                'etag': a.etag,
                'size_bytes': a.size_bytes,
            })
        items.sort(key=lambda x: (x['kind'], x['bucket'], x['key']))
        payload = json.dumps(items, ensure_ascii=False, separators=(',', ':'), sort_keys=True).encode('utf-8')
        return hashlib.sha256(payload).hexdigest()

    def compute_fingerprint(self, scan_id: str) -> str:
        arts = self.list_raw_artifacts(scan_id)
        return self._fingerprint_raw_inputs(arts)

    def list_raw_artifacts(self, scan_id: str) -> list[Artifact]:
        with self.session() as db:
            res = db.execute(
                    select(Artifact).where(
                        Artifact.scan_id == scan_id,
                        Artifact.schema_version.is_(None),
                        Artifact.status == 'AVAILABLE',
                    )
                ).scalars().all()
            for a in res:
                db.expunge(a)
            return cast(list[Artifact], res)

    def find_ingest_run(
            self,
            *,
            company_id: str,
            scan_id: str,
            schema_version: str,
            input_fingerprint: str,
    ) -> IngestRun | None:
        with self.session() as db:
            return db.execute(
                select(IngestRun).where(
                    IngestRun.company_id == company_id,
                    IngestRun.scan_id == scan_id,
                    IngestRun.schema_version == schema_version,
                    IngestRun.input_fingerprint == input_fingerprint,
                ).order_by(IngestRun.id.desc())
            ).scalars().first()

    def create_ingest_run(self,
                          *,
                          company_id: str,
                          scan_id: str,
                          schema_version: str,
                          input_fingerprint: str,
                          status: str = 'QUEUED',
                          ) -> int:
        with self.session() as db:
            run = IngestRun(
                company_id=company_id,
                scan_id=scan_id,
                schema_version=schema_version,
                input_fingerprint=input_fingerprint,
                status=status,
                error={},
                finished_at=None,
            )
            db.add(run)
            db.flush()
            return int(run.id)

    def set_ingest_run_status(
            self,
            *,
            run_id: int,
            status: str,
            error: dict | None = None,
            set_finished_at: bool = False,
        ) -> None:
        with self.session() as db:
            run = db.get(IngestRun, run_id)
            if not run:
                raise RuntimeError(f"IngestRun {run_id} not found")
            run.status = status
            if error is not None:
                run.error = error
            if set_finished_at:
                run.finished_at = datetime.now(timezone.utc)

    def list_queued_ingest_runs(
            self,
            *,
            schema_version: str | None = None,
            company_id: str | None = None,
            limit: int = 10,
    ): # list[IngestRun]
        with self.session() as db:
            q = select(IngestRun).where(IngestRun.status == 'QUEUED')
            if schema_version:
                q = q.where(IngestRun.schema_version == schema_version)
            if company_id:
                q = q.where(IngestRun.company_id == company_id)
            q = q.order_by(IngestRun.id.asc()).limit(limit)
            runs = db.execute(q).scalars().all()
            for r in runs:
                db.expunge(r)
            return runs

    def claim_ingest_run(self, run_id: int) -> bool:
        with self.session() as db:
            res = db.execute(
                update(IngestRun)
                .where(IngestRun.id == run_id, IngestRun.status == 'QUEUED')
                .values(status='RUNNING')
            )
            return bool(res.rowcount)

    def get_ingest_run(self, run_id: int)-> IngestRun:
        with self.session() as db:
            run = db.get(IngestRun, run_id)
            if not run:
                raise RuntimeError(f"IngestRun {run_id} not found")
            db.expunge(run)
            return run

    def list_scans_by_dataset_version(self, dataset_version_id: str):
        with self.session() as db:
            scans = db.execute(
                select(Scan).where(Scan.dataset_version_id == dataset_version_id)
            ).scalars().all()
            for s in scans:
                db.expunge(s)
            return scans

    def list_scan_poses_by_dataset_version(self, dataset_version_id: str) -> list[dict]:
        """
        Returns rows as plain dicts:
          [{"scan_id": "...", "pose": {...}, "quality": 0, "meta": {...}}, ...]
        """
        with self.session() as db:
            rows = db.execute(
                select(ScanPose.scan_id, ScanPose.pose, ScanPose.quality, ScanPose.meta)
                .where(ScanPose.dataset_version_id == dataset_version_id)
            ).all()

        return [
            {
                "scan_id": scan_id,
                "pose": pose,
                "quality": int(quality or 0),
                "meta": meta or {},
            }
            for (scan_id, pose, quality, meta) in rows
        ]

    def upsert_derived_artifact(
            self,
            *,
            company_id: str,
            scan_id: str,
            kind: str,
            schema_version: str,
            s3_bucket: str,
            s3_key: str,
            etag: str | None,
            size_bytes: int | None,
            status: str = "READY",
            meta: dict | None = None,
    ) -> None:
        meta = meta or {}
        with self.session() as db:
            existing = db.execute(
                select(Artifact).where(
                    Artifact.scan_id == scan_id,
                    Artifact.kind == kind,
                    Artifact.schema_version == schema_version,
                )
            ).scalars().first()

            if existing:
                existing.s3_bucket = s3_bucket
                existing.s3_key = s3_key
                existing.etag = etag
                existing.size_bytes = size_bytes
                existing.status = status
                existing.meta = meta
                return

            db.add(Artifact(
                company_id=company_id,
                scan_id=scan_id,
                kind=kind,
                schema_version=schema_version,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                etag=etag,
                size_bytes=size_bytes,
                status=status,
                meta=meta,
            ))

    def find_derived_artifact(self, scan_id: str, kind: str, schema_version: str) -> Artifact | None:
        with self.session() as db:
            art = db.execute(
                select(Artifact).where(
                    Artifact.scan_id == scan_id,
                    Artifact.kind == kind,
                    Artifact.schema_version == schema_version,
                )
            ).scalars().first()
            if art:
                db.expunge(art)
            return art

    def add_scan_edges(self, company_id: str, dataset_version_id: str, edges: list[dict]) -> int:
        """
        Upsert edges by unique key (dataset_version_id, scan_id_from, scan_id_to, kind).
        Returns number of rows affected (best-effort).
        """
        if not edges:
            return 0

        rows = []
        for e in edges:
            rows.append({
                "company_id": company_id,
                "dataset_version_id": dataset_version_id,
                "scan_id_from": e["from"],
                "scan_id_to": e["to"],
                "kind": e.get("kind", "unknown"),
                "weight": float(e.get("weight", 1.0)),
                "transform_guess": e.get("transform_guess") or {},
                "meta": e.get("meta") or {},
            })

        with self.session() as db:
            stmt = pg_insert(ScanEdge).values(rows)

            # что считаем “лучше”, если прилетели повторно:
            # - weight: берём MAX (или просто новый)
            # - transform_guess/meta: берём новый (последний)
            update = {
                "weight": case(
            (ScanEdge.weight < stmt.excluded.weight, stmt.excluded.weight),
                    else_=ScanEdge.weight
            ),
                "transform_guess": stmt.excluded.transform_guess,
                "meta": stmt.excluded.meta,
                "updated_at": func.now(),
            }

            stmt = stmt.on_conflict_do_update(
                constraint="uq_scan_edges_dv_from_to_kind",
                set_=update,
            )

            res = db.execute(stmt)
            # rowcount для upsert может быть не идеален, но полезен для логов
            return int(res.rowcount or 0)

    def upsert_scan_pose(
            self,
            company_id: str,
            dataset_version_id: str,
            scan_id: str,
            pose: dict,
            quality: float = 0.0,
            meta: dict | None = None,
    ) -> None:
        meta = meta or {}

        with self.session() as db:
            stmt = pg_insert(ScanPose).values({
                "company_id": company_id,
                "dataset_version_id": dataset_version_id,
                "scan_id": scan_id,
                "pose": pose,
                "quality": float(quality),
                "meta": meta,
            }).on_conflict_do_update(
                constraint="uq_scan_poses_dv_scan",
                set_={
                    "pose": pg_insert(ScanPose).excluded.pose,
                    "quality": pg_insert(ScanPose).excluded.quality,
                    "meta": pg_insert(ScanPose).excluded.meta,
                    "updated_at": func.now(),
                },
            )

            db.execute(stmt)

    def list_scan_edges(self, dataset_version_id: str) -> list[ScanEdge]:
        with self.session() as db:
            return list(
                db.execute(
                    select(ScanEdge).where(ScanEdge.dataset_version_id == dataset_version_id)
                ).scalars().all()
            )
