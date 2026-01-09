"""
Command-line entry point for launching the ingest workflow.

This script parses command-line arguments to submit an IngestWorkflow
to Temporal. It demonstrates how to use the new ingest workflow with
the database-backed scan and artifact management system.

Example usage::

    python -m point_cloud.temporal.client.start_ingest \
        --company company1 \
        --dataset dataset1 \
        --dataset-name "My Dataset" \
        --crs crs1 \
        --artifact raw.point_cloud:/path/to/cloud.laz

Before running this script, ensure that a Temporal worker is running on
the ``point-cloud-task-queue`` task queue.
"""

from __future__ import annotations

import asyncio
import argparse
import time
from temporalio.client import Client
from temporalio.service import RPCError

from point_cloud.workflows.ingest_workflow import IngestWorkflowParams


async def run_ingest_workflow(
    company_id: str,
    dataset_id: str,
    *,
    crs_id: str | None = None,
    dataset_name: str | None = None,
    schema_version: str = "1.1.0",
    force: bool = False,
    artifacts: list[dict[str, str]] | None = None,
) -> None:
    """Start the ingest workflow and await its result."""

    client = await Client.connect("localhost:7233")

    params = IngestWorkflowParams(
        company_id=company_id,
        dataset_id=dataset_name,
        crs_id=crs_id,
        dataset_name=dataset_name,
        schema_version=schema_version,
        force=force,
        artifacts=artifacts or [],
    )

    workflow_id = f"ingest-{company_id}-{dataset_name}-{int(time.time())}"

    # Workflow name is constructed from VERSION in workflow file
    # It should match the name in @workflow.defn decorator
    handle = await client.start_workflow(
        'MVP-ingest',
        params,
        id=workflow_id,
        task_queue="point-cloud-task-queue",
    )

    # If artifacts were provided via signal, send them
    # (Alternatively, they can be provided in params)
    # if artifacts:
    #     await handle.signal('add_raw_artifacts', artifacts)

    try:
        result = await handle.result()
        print("Ingest workflow completed with result:")
        print(result)

    except RPCError as exc:
        print(f"Failed to get result from Temporal: {exc}")
        print(f"Workflow continues to run. Check its status in Temporal UI with ID {handle.id}.")


def parse_artifact_arg(artifact_str: str) -> dict[str, str]:
    """
    Parse artifact argument in format: kind:path[:filename]
    
    Examples:
        raw.point_cloud:/path/to/cloud.laz
        raw.trajectory:/path/to/path.txt
        raw.control_point:/path/to/cp.txt:ControlPoint.txt
    """
    parts = artifact_str.split(':', 2)
    if len(parts) < 2:
        raise ValueError(f"Invalid artifact format: {artifact_str}. Expected kind:path[:filename]")
    
    kind = parts[0]
    local_file_path = parts[1]
    filename = parts[2] if len(parts) > 2 else None
    
    return {
        'kind': kind,
        'local_file_path': local_file_path,
        'filename': filename,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Start the ingest workflow for point cloud data.")
    parser.add_argument(
        "--company",
        required=True,
        help="Company ID",
    )
    parser.add_argument(
        "--dataset-name",
        required=True,
        help="Dataset ID",
    )
    parser.add_argument(
        "--dataset-name",
        help="Dataset name (optional, required if creating new dataset)",
    )
    parser.add_argument(
        "--crs",
        help="CRS ID (optional, required if creating new dataset)",
    )
    parser.add_argument(
        "--schema-version",
        default="1.1.0",
        help="Schema version (default: 1.1.0)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force creation even if ingest run already exists",
    )
    parser.add_argument(
        "--artifact",
        action="append",
        dest="artifacts",
        help=(
            "Artifact to upload in format: kind:path[:filename]. "
            "Can be specified multiple times. "
            "Example: raw.point_cloud:/path/to/cloud.laz"
        ),
    )
    args = parser.parse_args()

    artifacts = None
    if args.artifacts:
        artifacts = [parse_artifact_arg(a) for a in args.artifacts]

    asyncio.run(
        run_ingest_workflow(
            company_id=args.company,
            dataset_id=args.dataset,
            crs_id=args.crs,
            dataset_name=args.dataset_name,
            schema_version=args.schema_version,
            force=args.force,
            artifacts=artifacts,
        )
    )


if __name__ == "__main__":
    main()

