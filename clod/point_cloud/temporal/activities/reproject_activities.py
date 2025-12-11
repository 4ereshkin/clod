"""
Activities for coordinate system transformations of point clouds.

The underlying logic leverages the :class:`SRS` class from
``point_cloud.reproject`` to perform PDAL based reprojection of LAS/LAZ
files.  Each call returns the path to the newly created reprojected
file or ``None`` if the operation failed.
"""

from __future__ import annotations

import asyncio
from typing import Optional, Dict, Any
from temporalio import activity

from clod.reproject import SRS

# region agent log
import json
import time

DEBUG_LOG_PATH = r"d:\4. Иное по работе\lidar-project\.cursor\debug.log"


def _agent_log(hypothesis_id: str, message: str, data: Dict[str, Any]) -> None:
    payload = {
        "sessionId": "debug-session",
        "runId": "pre-fix-1",
        "hypothesisId": hypothesis_id,
        "location": "reproject_activities.py",
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass
# endregion


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
        _agent_log(
            "H2",
            "reproject start",
            {"file_path": file_path, "in_srs": in_srs, "out_srs": out_srs},
        )
        srs = SRS(cloud_path=file_path, in_srs=in_srs, out_srs=out_srs)
        result = srs.run()
        _agent_log(
            "H2",
            "reproject done",
            {"file_path": file_path, "result_path": result},
        )
        return result

    return await loop.run_in_executor(None, _reproject)