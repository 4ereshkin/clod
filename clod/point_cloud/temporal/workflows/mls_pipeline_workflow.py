"""
Workflow for orchestration of point cloud processing.

This workflow coordinates a sequence of activities to extract metadata
from raw point cloud files, reproject them into a common spatial
reference system, insert the results into a database and optionally
generate 3D tiles.  The workflow is designed to be idempotent and
replayable, making use of Temporal's deterministic execution model and
retriable activities.

The workflow accepts a list of file paths and optional parameters for
coordinate system transformation and database configuration.  It
returns a summary of the operations performed, including the paths of
the reprojected files and whether tile generation succeeded for each.
"""

from __future__ import annotations

from typing import List, Dict, Any
from temporalio import workflow
from temporalio.common import RetryPolicy

from clod.point_cloud.temporal.activities import (
    load_metadata_for_files,
    reproject_file,
    insert_file_into_db,
    convert_to_tileset,
)


@workflow.defn
class MlsPipelineWorkflow:
    """A Temporal workflow that runs a simple point cloud processing pipeline."""

    @workflow.run
    async def run(
        self,
        file_paths: List[str],
        *,
        in_srs: str = "EPSG:4490",
        out_srs: str = "EPSG:4326",
        db_config_path: str = "db.json",
        generate_tiles: bool = False,
    ) -> Dict[str, Any]:
        """
        Execute the pipeline on the provided files.

        Parameters
        ----------
        file_paths:
            A list of LAS/LAZ file paths to process.  These paths must be
            accessible to the worker executing the activities.
        in_srs:
            EPSG or PROJ string describing the source coordinate system.
        out_srs:
            EPSG or PROJ string describing the target coordinate system.
        db_config_path:
            Path to the JSON configuration file for database access.
        generate_tiles:
            If ``True``, the workflow will produce Cesium 3D tiles for each
            processed file.

        Returns
        -------
        dict
            A dictionary summarising the results of the pipeline:
            ``metadata`` contains the extracted metadata, ``reprojected_files``
            lists the new file paths and ``tiles_generated`` indicates
            whether tile generation succeeded for each file (if enabled).
        """
        # Step 1: extract and store metadata for the incoming files.  This
        # activity writes JSON files to disk and returns their locations.
        meta_result = await workflow.execute_activity(
            load_metadata_for_files,
            file_paths,
            schedule_to_close_timeout=600,
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        # Step 2: reproject each file concurrently.  The results list may
        # contain ``None`` for files that failed to reproject; filter those
        # out for subsequent steps.
        reproject_futures = []
        for file_path in file_paths:
            fut = workflow.execute_activity(
                reproject_file,
                file_path,
                in_srs,
                out_srs,
                schedule_to_close_timeout=3600,
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            reproject_futures.append(fut)
        reproject_results = await workflow.await_all(reproject_futures)
        reprojected_files = [res for res in reproject_results if res]

        # Step 3: insert each reprojected file into the database.  Run
        # sequentially to avoid overwhelming the database connection pool.
        for file_path in reprojected_files:
            await workflow.execute_activity(
                insert_file_into_db,
                file_path,
                db_config_path,
                schedule_to_close_timeout=3600,
                retry_policy=RetryPolicy(maximum_attempts=3),
            )

        # Step 4: optionally generate 3D tiles for each file.  Use
        # concurrency here as the tile conversion is CPU bound.  If tile
        # generation is disabled, this will simply return an empty list.
        tiles_results: List[bool] = []
        if generate_tiles:
            tile_futures = []
            for file_path in reprojected_files:
                fut = workflow.execute_activity(
                    convert_to_tileset,
                    file_path,
                    "cesium_tiles",
                    schedule_to_close_timeout=3600,
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
                tile_futures.append(fut)
            tiles_results = await workflow.await_all(tile_futures)

        # Summarise the results for the caller.  Workflows should return
        # simple data structures that can be JSON encoded.
        return {
            "metadata": meta_result,
            "reprojected_files": reprojected_files,
            "tiles_generated": tiles_results,
        }