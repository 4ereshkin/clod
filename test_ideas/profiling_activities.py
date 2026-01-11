from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List

from temporalio import activity


@activity.defn
async def meta_and_info():
