"""Deprecated compatibility module.

This file used to contain a local reprojection script and is kept only to avoid
breaking imports. Use `port.tools.reproject_paths` instead.
"""

from port.tools.reproject_paths import process_files, is_data_line

__all__ = ["process_files", "is_data_line"]
