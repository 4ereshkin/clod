"""
Activities for reading database records associated with point cloud loads.

The legacy :class:`Records` class reads rows from the ``clouds`` and
``loads`` tables, merging them into a pandas DataFrame.  This activity
invokes that logic and returns the resulting rows as a list of
dictionaries for safe JSON serialisation.
"""

from __future__ import annotations

import asyncio
from typing import List, Dict, Any, Optional
from temporalio import activity
import pandas as pd

from clod.records import Records


@activity.defn
async def read_records_table(
    config_path: str,
    records_table: str = "loads",
) -> List[Dict[str, Any]]:
    """
    Read the merged records from the database.

    Parameters
    ----------
    config_path:
        Path to the JSON configuration file with database credentials.
    records_table:
        Name of the records table to read; default is ``loads``.

    Returns
    -------
    list of dict
        A list of dictionaries representing the rows returned from the
        database, one per record.
    """
    loop = asyncio.get_running_loop()

    def _read() -> List[Dict[str, Any]]:
        rec = Records(config_path=config_path, records_table=records_table)
        # Authenticate manually instead of relying on run() to avoid side effects
        if not rec.auth():
            return []
        if not rec._read_records():
            return []
        df: Optional[pd.DataFrame] = rec.records_df
        if df is None:
            return []
        return df.to_dict(orient="records")

    return await loop.run_in_executor(None, _read)