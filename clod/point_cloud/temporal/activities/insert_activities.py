"""
Activities for inserting point cloud data into a PostGIS database.

These activities wrap the legacy :class:`Insert` class from
``point_cloud.insert``.  The activity authenticates with the database
using the provided configuration file and writes data into the
``clouds`` and ``metadata`` tables.  A boolean success flag is
returned to the workflow.
"""

from __future__ import annotations

import asyncio
from typing import Optional
from temporalio import activity

from clod.insert import Insert


@activity.defn
async def insert_file_into_db(
    file_path: str,
    config_path: Optional[str] = "clod/db.json",
) -> bool:
    """
    Import a single LAS/LAZ file into the database.

    Parameters
    ----------
    file_path:
        Path to the LAS/LAZ file to import.
    config_path:
        Path to the JSON configuration file with database credentials.

    Returns
    -------
    bool
        ``True`` if the import succeeded, ``False`` otherwise.
    """
    loop = asyncio.get_running_loop()

    def _insert() -> bool:
        storage = Insert(config_path=config_path)
        storage.cloud_path = file_path
        activity.heartbeat({"file_path": file_path, "status": "Insert started"})
        status = storage.run()

        if not status:
            activity.heartbeat({"file_path": file_path, "status": "Insert failed"})

        activity.heartbeat({"file_path": file_path, "status": "Insert completed"})

        return status

    return await loop.run_in_executor(None, _insert)