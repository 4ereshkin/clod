from sqlalchemy import (
    Column, Integer, BigInteger, ForeignKey, Float,
    Text, JSON, DateTime, String, Boolean, UniqueConstraint, Index
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class Company(Base):
    __tablename__ = "companies"
    __table_args__ = {"schema": "core"}

    id = Column(Text, primary_key=True)
    name = Column(Text, nullable=False)


class CRS(Base):
    __tablename__ = "crs"
    __table_args__ = {"schema": "core"}

    id = Column(Text, primary_key=True)
    name = Column(Text, nullable=False)
    zone_degree = Column(Integer, nullable=False)
    epsg = Column(Integer, nullable=True)
    units = Column(Text, nullable=False)
    axis_order = Column(Text, nullable=False)
    meta = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Dataset(Base):
    __tablename__ = "datasets"
    __table_args__ = (
        UniqueConstraint('company_id', 'name', name='uq_datasets_company_name'),
        Index("ix_datasets_company_id", "company_id"),
        Index("ix_datasets_crs_id", "crs_id"),
        {"schema": "core"},
    )

    id = Column(Text, primary_key=True)
    company_id = Column(Text, ForeignKey("core.companies.id"), nullable=False)
    name = Column(Text, nullable=False)
    crs_id = Column(Text, ForeignKey("core.crs.id"), nullable=False)

class DatasetVersion(Base):
    __tablename__ = "dataset_versions"
    __table_args__ = (
        UniqueConstraint("dataset_id", "version", name="uq_dataset_versions_dataset_version"),
        {"schema": "core"},
    )

    id = Column(Text, primary_key=True)
    dataset_id = Column(Text, ForeignKey('core.datasets.id'), nullable=False)

    version = Column(Integer, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Scan(Base):
    __tablename__ = "scans"
    __table_args__ = (
        Index("ix_scans_company_id", "company_id"),
        Index("ix_scans_dataset_version_id", "dataset_version_id"),
        Index("ix_scans_crs_id", "crs_id"),
        {"schema": "core"},
    )

    id = Column(Text, primary_key=True)
    company_id = Column(Text, ForeignKey("core.companies.id"), nullable=False)

    dataset_id = Column(Text, ForeignKey('core.datasets.id'), nullable=False)
    dataset_version_id = Column(Text, ForeignKey('core.dataset_versions.id'), nullable=False)

    crs_id = Column(Text, ForeignKey("core.crs.id"), nullable=False)

    status = Column(Text, nullable=False, default="CREATED")
    schema_version = Column(Text, nullable=False, default="1.1.0")

    owner_department_id = Column(Text, nullable=True)

    meta = Column(JSON, nullable=False, default=dict)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now())

class Artifact(Base):
    __tablename__ = "artifacts"
    __table_args__ = (
        Index("ix_artifacts_company_id", "company_id"),
        Index("ix_artifacts_scan_id", "scan_id"),
        Index("ix_artifacts_kind", "kind"),
        Index("ix_artifacts_scan_id_kind", "scan_id", "kind"),
        {"schema": "core"},
    )

    id = Column(BigInteger, primary_key=True)
    company_id = Column(Text, ForeignKey("core.companies.id"), nullable=False)
    scan_id = Column(String,
                     ForeignKey("core.scans.id", ondelete='CASCADE'),
                     nullable=False)
    kind = Column(Text, nullable=False)
    schema_version = Column(Text, nullable=True)
    s3_bucket = Column(Text, nullable=False)
    s3_key = Column(Text, nullable=False)
    etag = Column(Text)
    size_bytes = Column(BigInteger)
    status = Column(Text, nullable=False)
    meta = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class IngestRun(Base):
    __tablename__ = "ingest_runs"
    __table_args__ = {"schema": "core"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    company_id = Column(Text, ForeignKey('core.companies.id'), nullable=False)
    scan_id = Column(Text, ForeignKey('core.scans.id'), nullable=False)

    schema_version = Column(Text, nullable=False)
    input_fingerprint = Column(Text, nullable=False)

    status = Column(Text, nullable=False)
    error = Column(JSONB, nullable=False, server_default='{}')

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    finished_at = Column(DateTime(timezone=True), nullable=True)


class ScanEdge(Base):
    __tablename__ = "scan_edges"
    __table_args__ = (
        UniqueConstraint(
            "dataset_version_id", "scan_id_from", "scan_id_to", "kind",
            name="uq_scan_edges_dv_from_to_kind"
        ),
        Index("ix_scan_edges_dv", "dataset_version_id"),
        {"schema": "core"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    company_id = Column(Text, ForeignKey("core.companies.id"), nullable=False)
    dataset_version_id = Column(Text, ForeignKey("core.dataset_versions.id"), nullable=False)

    scan_id_from = Column(Text, ForeignKey("core.scans.id", ondelete="CASCADE"), nullable=False)
    scan_id_to   = Column(Text, ForeignKey("core.scans.id", ondelete="CASCADE"), nullable=False)

    kind = Column(Text, nullable=False)                 # "traj_tail_head", etc.
    weight = Column(Float, nullable=False, default=1) # или Float, см. ниже
    transform_guess = Column(JSONB, nullable=False, server_default="{}")
    meta = Column(JSONB, nullable=False, server_default="{}")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class ScanPose(Base):
    __tablename__ = "scan_poses"
    __table_args__ = (
        UniqueConstraint("dataset_version_id", "scan_id", name="uq_scan_poses_dv_scan"),
        Index("ix_scan_poses_dv", "dataset_version_id"),
        {"schema": "core"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    company_id = Column(Text, ForeignKey("core.companies.id"), nullable=False)
    dataset_version_id = Column(Text, ForeignKey("core.dataset_versions.id"), nullable=False)
    scan_id = Column(Text, ForeignKey("core.scans.id", ondelete="CASCADE"), nullable=False)

    pose = Column(JSONB, nullable=False)               # {"t":[...], "R":[...]}
    quality = Column(Integer, nullable=False, default=0)  # или Float
    meta = Column(JSONB, nullable=False, server_default="{}")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
