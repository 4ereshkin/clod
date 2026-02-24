#!/usr/bin/env python3
r"""
Запуск кластеризации для dataset_version без полного пайплайна.

Пример:
    python scripts/run_cluster_pipeline.py \
        --dataset-version-id <dataset_version_id> \
        --schema-version 1.1.0
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
    if os.getenv("WORKFLOW_VERSION", "MVP"):
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


async def run_cluster_pipeline(params: "ClusterPipelineParams") -> None:
    from point_cloud.workflows.cluster_workflow import ClusterPipeline

    client = await Client.connect("localhost:7233")
    workflow_id = f"cluster-pipeline-{params.dataset_version_id}-{int(time.time())}"

    handle = await client.start_workflow(
        f"{ClusterPipeline.__temporal_workflow_definition.name}",
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
    print(f"- merged_file: {result.get('merged_file')}")
    print(f"- tile_count: {result.get('tile_count')}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Запуск ClusterPipeline для указанного dataset_version_id."
    )
    parser.add_argument("--dataset-version-id", required=True, help="Dataset version ID")
    parser.add_argument("--schema-version", default="1.1.0", help="Schema version")
    parser.add_argument(
        "--dst-dir",
        default="point_cloud/tmp/cluster",
        help="Local directory for cluster artifacts",
    )
    parser.add_argument("--tile-size", type=float, default=50.0, help="Tile size (meters)")
    parser.add_argument(
        "--splitter-buffer",
        type=float,
        default=3.0,
        help="Splitter buffer (meters)",
    )

    if len(sys.argv) == 1:
        parser.print_help()
        return

    args = parser.parse_args()
    ensure_workflow_version()

    from point_cloud.workflows.cluster_workflow import ClusterPipelineParams

    params = ClusterPipelineParams(
        dataset_version_id=args.dataset_version_id,
        schema_version=args.schema_version,
        dst_dir=args.dst_dir,
        tile_size=args.tile_size,
        splitter_buffer=args.splitter_buffer,
    )

    asyncio.run(run_cluster_pipeline(params))


if __name__ == "__main__":
    main()
