#!/usr/bin/env python3
r"""
Полный end-to-end воркфлоу: ingest N сканов -> profiling -> reproject -> preprocess
-> registration -> export.

Можно включить prod-registration через флаг --use-prod-registration.

Пример:
    python scripts/run_full_pipeline.py \
        --company demo \
        --dataset "railway" \
        --dataset-crs "CGCS2000" \
        --target-srs "EPSG:4326" \
        --scan "cloud=/data/scan1.laz;path=/data/scan1/path.txt;cp=/data/scan1/ControlPoint.txt" \
        --scan "cloud=/data/scan2.laz;path=/data/scan2/path.txt;cp=/data/scan2/ControlPoint.txt"
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
    if "cloud" not in values:
        raise ValueError("Scan spec must include cloud=<path>")
    return values


def build_artifacts(spec: dict[str, str]) -> list[dict[str, str]]:
    artifacts = [
        {"kind": "raw.point_cloud", "local_file_path": str(Path(spec["cloud"]).absolute())}
    ]
    if spec.get("path"):
        artifacts.append({
            "kind": "raw.trajectory",
            "local_file_path": str(Path(spec["path"]).absolute()),
        })
    if spec.get("cp"):
        artifacts.append({
            "kind": "raw.control_point",
            "local_file_path": str(Path(spec["cp"]).absolute()),
        })
    return artifacts


def validate_files(spec: dict[str, str]) -> None:
    cloud_path = Path(spec["cloud"])
    if not cloud_path.exists():
        raise FileNotFoundError(f"Cloud file not found: {cloud_path}")
    if spec.get("path") and not Path(spec["path"]).exists():
        raise FileNotFoundError(f"Path file not found: {spec['path']}")
    if spec.get("cp") and not Path(spec["cp"]).exists():
        raise FileNotFoundError(f"ControlPoint file not found: {spec['cp']}")


async def run_full_pipeline(params: "FullPipelineParams") -> None:
    from point_cloud.workflows.full_pipeline_workflow import FullPipelineWorkflow

    client = await Client.connect("localhost:7233")
    workflow_id = f"full-pipeline-{params.company_id}-{params.dataset_name}-{int(time.time())}"

    handle = await client.start_workflow(
        f"{FullPipelineWorkflow.__temporal_workflow_definition.name}",
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
    if result.get("clustering_result") is not None:
        print(f"- clustering_result: {result.get('clustering_result')}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Полный end-to-end workflow для демонстрации (ingest -> profiling -> reproject -> preprocess -> registration)."
    )
    parser.add_argument("--company", required=True, help="Company ID")
    parser.add_argument("--dataset", required=True, help="Dataset name")
    parser.add_argument("--dataset-crs", required=True, help="CRS ID for dataset (e.g. CGCS2000)")
    parser.add_argument("--target-srs", required=True, help="Target SRS (e.g. EPSG:4326)")
    parser.add_argument(
        "--scan",
        action="append",
        required=True,
        help="Scan spec: cloud=<path>[;path=<path>][;cp=<path>]. Can be repeated.",
    )
    parser.add_argument("--schema-version", default="1.1.0", help="Schema version")
    parser.add_argument("--bump-version", action="store_true", help="Create a new dataset version")
    parser.add_argument("--force", action="store_true", help="Force registration export overwrite")
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
    parser.add_argument(
        "--run-clustering",
        action="store_true",
        help="Run clustering workflow after registration",
    )

    if len(sys.argv) == 1:
        parser.print_help()
        return

    args = parser.parse_args()
    ensure_workflow_version()

    from point_cloud.workflows.full_pipeline_workflow import (
        FullPipelineParams,
        FullPipelineScan,
    )

    scan_specs = []
    for spec in args.scan:
        parsed = parse_scan_spec(spec)
        validate_files(parsed)
        scan_specs.append(parsed)

    scans = [FullPipelineScan(artifacts=build_artifacts(spec)) for spec in scan_specs]

    params = FullPipelineParams(
        company_id=args.company,
        dataset_name=args.dataset,
        dataset_crs_id=args.dataset_crs,
        target_srs=args.target_srs,
        bump_version=args.bump_version,
        schema_version=args.schema_version,
        force=args.force,
        scans=scans,
        profiling_cloud_dir=args.profiling_cloud_dir,
        profiling_geojson_dir=args.profiling_geojson_dir,
        preprocessing_voxel_size_m=args.voxel_size,
        preprocessing_mean_k=args.mean_k,
        preprocessing_multiplier=args.multiplier,
        use_prod_registration=args.use_prod_registration,
        run_clustering=args.run_clustering,
    )

    asyncio.run(run_full_pipeline(params))


if __name__ == "__main__":
    main()
