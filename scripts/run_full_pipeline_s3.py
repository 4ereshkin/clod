#!/usr/bin/env python3
r"""
Полный end-to-end воркфлоу по S3-ключам: ingest N сканов -> profiling -> reproject
-> preprocess -> registration.

Пример:
    python scripts/run_full_pipeline_s3.py \
        --company demo \
        --dataset "railway" \
        --source-srs "EPSG:4490" \
        --target-srs "EPSG:4326" \
        --scan "cloud=tenants/demo/scan1.laz;path=tenants/demo/scan1/path.txt" \
        --scan "cloud=tenants/demo/scan2.laz;path=tenants/demo/scan2/path.txt"
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

from temporalio.client import Client
from temporalio.service import RPCError
import yaml

# add project root to PYTHONPATH
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


def ensure_workflow_version() -> None:
    if os.environ.get("WORKFLOW_VERSION"):
        return
    config_path = project_root / "scripts" / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(
            "WORKFLOW_VERSION is not set and scripts/config.yaml is missing"
        )
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    version = (data.get("VERSION_INFO") or {}).get("WORKFLOW_VERSION")
    if not version:
        raise ValueError("WORKFLOW_VERSION missing in scripts/config.yaml")
    os.environ["WORKFLOW_VERSION"] = str(version)


def parse_scan_spec(spec: str) -> dict:
    parts = [p.strip() for p in spec.split(";") if p.strip()]
    values: dict[str, str] = {}
    for part in parts:
        if "=" not in part:
            raise ValueError(f"Invalid scan spec segment: {part}")
        key, value = part.split("=", 1)
        values[key.strip().lower()] = value.strip()
    if "cloud" not in values or "path" not in values:
        raise ValueError("Scan spec must include cloud=<s3_key> and path=<s3_key>")
    return values


async def run_full_pipeline(params: "FullPipelineS3Params") -> None:
    from point_cloud.workflows.full_pipeline_s3_workflow import FullPipelineS3Workflow

    client = await Client.connect("localhost:7233")
    workflow_id = f"full-pipeline-s3-{params.company_id}-{params.dataset_name}-{int(time.time())}"

    handle = await client.start_workflow(
        f"{FullPipelineS3Workflow.__temporal_workflow_definition.name}",
        params,
        id=workflow_id,
        task_queue="point-cloud-task-queue",
    )

    print(f"STARTED: {workflow_id}")
    try:
        result = await handle.result()
    except RPCError as exc:
        print(f"⚠ Не удалось получить результат из Temporal: {exc}")
        print(f"Workflow продолжает выполняться. Проверьте статус по ID {handle.id}")
        return

    print("RESULT:")
    print(f"- dataset_version_id: {result.get('dataset_version_id')}")
    print(f"- scans: {len(result.get('scan_ids', []))}")
    print(f"- registration_result: {result.get('registration_result')}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Полный end-to-end workflow из S3 (ingest -> profiling -> reproject -> preprocess -> registration)."
        )
    )
    parser.add_argument("--company", required=True, help="Company ID")
    parser.add_argument("--dataset", required=True, help="Dataset name")
    parser.add_argument("--source-srs", required=True, help="Input SRS (e.g. EPSG:4490)")
    parser.add_argument("--target-srs", required=True, help="Target SRS (e.g. EPSG:4326)")
    parser.add_argument(
        "--scan",
        action="append",
        required=True,
        help="Scan spec: cloud=<s3_key>;path=<s3_key>. Can be repeated.",
    )
    parser.add_argument("--schema-version", default="1.1.0", help="Schema version")
    parser.add_argument("--bump-version", action="store_true", help="Create a new dataset version")
    parser.add_argument("--force", action="store_true", help="Force registration export overwrite")
    parser.add_argument(
        "--s3-bucket",
        default=None,
        help="Optional S3 bucket override (defaults to settings.s3_bucket)",
    )
    parser.add_argument(
        "--profiling-cloud-dir",
        default="point_cloud/tmp/profiling",
        help="Local dir for downloading clouds during profiling",
    )
    parser.add_argument(
        "--profiling-geojson-dir",
        default="point_cloud/tmp/hexbin",
        help="Local dir for profiling hexbin GeoJSON",
    )
    parser.add_argument("--voxel-size", type=float, default=0.10, help="Preprocess voxel size (meters)")
    parser.add_argument("--mean-k", type=int, default=20, help="Preprocess outlier mean_k")
    parser.add_argument("--multiplier", type=float, default=2.0, help="Preprocess outlier multiplier")
    parser.add_argument(
        "--use-prod-registration",
        action="store_true",
        help="Use prod registration workflow instead of registration-solver",
    )

    if len(sys.argv) == 1:
        parser.print_help()
        return

    args = parser.parse_args()
    ensure_workflow_version()

    from point_cloud.workflows.full_pipeline_s3_workflow import (
        FullPipelineS3Params,
        FullPipelineS3Scan,
    )

    scan_specs = [parse_scan_spec(spec) for spec in args.scan]
    scans = [
        FullPipelineS3Scan(
            cloud_s3_key=spec["cloud"],
            path_s3_key=spec["path"],
        )
        for spec in scan_specs
    ]

    params = FullPipelineS3Params(
        company_id=args.company,
        dataset_name=args.dataset,
        source_srs=args.source_srs,
        target_srs=args.target_srs,
        bump_version=args.bump_version,
        schema_version=args.schema_version,
        force=args.force,
        scans=scans,
        s3_bucket=args.s3_bucket,
        profiling_cloud_dir=args.profiling_cloud_dir,
        profiling_geojson_dir=args.profiling_geojson_dir,
        preprocessing_voxel_size_m=args.voxel_size,
        preprocessing_mean_k=args.mean_k,
        preprocessing_multiplier=args.multiplier,
        use_prod_registration=args.use_prod_registration,
    )

    asyncio.run(run_full_pipeline(params))


if __name__ == "__main__":
    main()
