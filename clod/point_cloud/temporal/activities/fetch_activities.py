"""
Activities for downloading point cloud data from the database.

Wraps the :class:`Fetch` class from ``point_cloud.fetch``.  The
activity authenticates with the database and writes the fetched cloud
file to the specified directory.
"""

from __future__ import annotations

import asyncio
from typing import Optional
from temporalio import activity

from fetch import Fetch


@activity.defn
async def fetch_file_from_db(
    load_id: str,
    file_name: str,
    config_path: Optional[str] = "point_cloud/temporal/db.json",
    save_path: str = "clod/result_cloud/fetched_clouds",
) -> bool:
    """
    Download a cloud from the database using its load ID.

    Parameters
    ----------
    load_id:
        Identifier of the load to fetch.
    file_name:
        The filename to use when saving the downloaded LAS/LAZ file.
    config_path:
        Path to the JSON configuration file with database credentials.
    save_path:
        Directory where the downloaded file should be written.

    Returns
    -------
    bool
        ``True`` on successful download, ``False`` otherwise.
    """
    loop = asyncio.get_running_loop()

    def _fetch() -> bool:
        fetcher = Fetch(
            config_path=config_path,
            load_id=load_id,
            cloud_path=file_name,
            save_path=save_path,
        )
        return fetcher.run()

    return await loop.run_in_executor(None, _fetch)