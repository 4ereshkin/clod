"""
Activities for coordinate system transformations of point clouds.

Each call runs a PDAL reprojection pipeline for LAS/LAZ files and returns
the path to the newly created reprojected file or ``None`` if the
operation failed.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional

import pdal
from temporalio import activity


@activity.defn
async def reproject_file(
    file_path: str,
    in_srs: str,
    out_srs: str,
) -> Optional[str]:
    """
    Reproject a single LAS/LAZ file to a different spatial reference system.

    Parameters
    ----------
    file_path:
        The path to the source LAS/LAZ file.
    in_srs:
        EPSG code or PROJ string of the input spatial reference system.
    out_srs:
        EPSG code or PROJ string of the desired output spatial reference system.

    Returns
    -------
    Optional[str]
        The path to the reprojected file on success, otherwise ``None``.
    """
    loop = asyncio.get_running_loop()

    def _reproject() -> Optional[str]:
        local_in = Path(file_path)
        local_out = local_in.with_name(
            f"{local_in.stem}__{out_srs.replace(':', '_')}{local_in.suffix}"
        )

        pipeline = {
            "pipeline": [
                {"type": "readers.las", "filename": str(local_in)},
                {"type": "filters.reprojection", "in_srs": in_srs, "out_srs": out_srs},
                {"type": "writers.las", "filename": str(local_out), "compression": "laszip"},
            ]
        }

        pipe = pdal.Pipeline(json.dumps(pipeline))
        try:
            pipe.execute()
        except Exception as exc:
            raise RuntimeError(f"PDAL reprojection failed: {exc}") from exc

        if not local_out.exists():
            raise RuntimeError(f"PDAL reprojection produced no output: {local_out}")

        return str(local_out)

    return await loop.run_in_executor(None, _reproject)
