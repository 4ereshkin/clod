"""
Activities for generating 3D Tiles using py3dtiles.

Wraps the :class:`CesiumImport` class from ``point_cloud.cesium`` which
uses the `py3dtiles` library to convert LAS/LAZ files into a 3D Tiles
tileset.  The activity returns a boolean indicating success.
"""

from __future__ import annotations

import asyncio
from typing import Optional
from temporalio import activity

from cesium import CesiumImport


@activity.defn
async def convert_to_tileset(
    cloud_path: str,
    output_dir: str = "cesium_tiles",
) -> bool:
    """
    Convert a LAS/LAZ file into a 3D Tiles tileset.

    Parameters
    ----------
    cloud_path:
        Path to the LAS/LAZ file to convert.
    output_dir:
        Directory where the generated tileset should be written.

    Returns
    -------
    bool
        ``True`` on successful conversion, otherwise ``False``.
    """
    loop = asyncio.get_running_loop()

    def _convert() -> bool:
        c = CesiumImport(cloud_path=cloud_path, output_dir=output_dir)
        return c.run()

    return await loop.run_in_executor(None, _convert)