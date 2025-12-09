"""
Activities related to metadata extraction and checkpointing.

These activities wrap the :class:`Checkpoint` class from the legacy
``point_cloud.checkpoint`` module.  Instead of prompting the user for
file selection via a GUI, the file paths are passed explicitly when the
activity is invoked.  The resulting metadata is written to the
``checkpoint_metadata`` directory as before and returned to the
workflow for subsequent processing.
"""

from __future__ import annotations

import asyncio
from typing import Dict, List, Any
from temporalio import activity

from point_cloud.checkpoint import Checkpoint


@activity.defn
async def load_metadata_for_files(file_paths: List[str]) -> Dict[str, Any]:
    """
    Extract metadata for a list of LAS/LAZ files and write it to disk.

    Parameters
    ----------
    file_paths:
        A list of absolute or relative paths to LAS/LAZ files whose
        PDAL metadata should be extracted.

    Returns
    -------
    dict
        A dictionary containing two keys:

        ``metadata_json_path``:
            A mapping from file stem to the path of the JSON metadata
            file written on disk.

        ``metadata``:
            A mapping from the original file path to the parsed
            metadata dictionary.
    """
    loop = asyncio.get_running_loop()

    def _extract() -> Dict[str, Any]:
        cp = Checkpoint()
        cp.file_path = file_paths
        cp.cloud_metadata = {}
        cp.metadata_json_path = {}
        for path in file_paths:
            cp.load_metadata(path)
        cp.metadata_to_json()
        return {
            "metadata_json_path": cp.metadata_json_path,
            "metadata": cp.cloud_metadata,
        }

    return await loop.run_in_executor(None, _extract)