"""
Command‑line entry point for launching the MLS point cloud pipeline.

This script parses command‑line arguments to determine which files to
process and submits a ``MlsPipelineWorkflow`` to Temporal.  It prints
the result returned by the workflow once completed.

Example usage::

    python -m point_cloud.temporal.client.start_pipeline \
        --files /data/cloud1.laz /data/cloud2.laz \
        --in-srs EPSG:4490 --out-srs EPSG:4326 \
        --generate-tiles

Before running this script, ensure that a Temporal worker is running on
the ``point-cloud-task-queue`` task queue.
"""

from __future__ import annotations

import asyncio
import argparse
import json
import time
from temporalio.client import Client
from temporalio.service import RPCError

from point_cloud.temporal.workflows.mls_pipeline_workflow import MlsPipelineWorkflow, MlsPipelineParams


async def run_workflow(
    file_paths: list[str],
    *,
    in_srs: str,
    out_srs: str,
    db_config_path: str,
    generate_tiles: bool,
) -> None:
    """Start the MLS pipeline workflow, send files via signal, and await its result."""

    client = await Client.connect("localhost:7233")

    params = MlsPipelineParams(
        in_srs=in_srs,
        out_srs=out_srs,
        db_config_path=db_config_path,
        generate_tiles=generate_tiles
    )

    if file_paths:
        workflow_id = f"mls-pipeline-{hash(tuple(file_paths)) & 0xFFFF:x}"
    else:
        workflow_id = f"mls-pipeline-{int(time.time())}"

    # Workflow name should match the name in @workflow.defn decorator (VERSION = 'MVP')
    handle = await client.start_workflow(
        'MVP',
        params,
        id=workflow_id,
        task_queue="point-cloud-task-queue",
    )

    files_to_send: list[str] = file_paths or []
    await handle.signal('las_selected', files_to_send)

    try:
        result = await handle.result()
        print("Workflow completed with result:")
        print(result)

    except RPCError as exc:
        print(f"Не дождались результата из Temporal: {exc}")
        print(f"Workflow всё равно продолжает выполняться. "
              f"Посмотри его статус в Temporal UI по ID {handle.id}.")


def main() -> None:
    parser = argparse.ArgumentParser(
                    description="Start the MLS point cloud pipeline workflow.")
    parser.add_argument(
        "--files",
        "-f",
        nargs="+",
        help=(
            "Paths to LAS/LAZ files to process. "
            "If omitted, files will be selected interactively via GUI on worker (las_choice activity)."
        ),
    )
    parser.add_argument(
        "--in-srs",
                    default="EPSG:4490",
                    help="Input spatial reference system (default: EPSG:4490)")
    parser.add_argument(
        "--out-srs",
                    default="EPSG:4326",
                    help="Output spatial reference system (default: EPSG:4326)")
    parser.add_argument(
        "--db-config-path",
                    default="clod/db.json",
                    help="Path to database configuration JSON (default: db.json)")
    parser.add_argument(
                    "--generate-tiles",
                    action="store_true",
                    help="Generate Cesium 3D tiles for each processed file")
    args = parser.parse_args()

    asyncio.run(
        run_workflow(
            file_paths=args.files,
            in_srs=args.in_srs,
            out_srs=args.out_srs,
            db_config_path=args.db_config_path,
            generate_tiles=args.generate_tiles,
        )
    )


if __name__ == "__main__":
    main()