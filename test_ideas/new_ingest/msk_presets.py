from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
from pathlib import Path

import yaml


@dataclass(frozen=True)
class MSKZonePreset:
    lon_0: float
    x_0: float
    y_0: float


@dataclass(frozen=True)
class MSKRegionPreset:
    zones: Dict[int, MSKZonePreset]
    gost_towgs84: Optional[str]


def load_msk_presets_yaml(path: str | Path) -> Dict[int, MSKRegionPreset]:
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "region" not in data:
        raise ValueError("MSK presets YAML: expected top-level key 'region'")

    region = data["region"]
    if not isinstance(region, dict):
        raise ValueError("MSK presets YAML: 'region' must be a mapping")

    out: Dict[int, MSKRegionPreset] = {}
    for region_key, region_val in region.items():
        reg_id = int(region_key)
        if not isinstance(region_val, dict):
            raise ValueError(f"MSK presets YAML: region {region_key} must be a mapping")

        gost = region_val.get("gost_towgs84")
        zones: Dict[int, MSKZonePreset] = {}

        for zone_key, zone_val in region_val.items():
            if zone_key == "gost_towgs84":
                continue
            z_id = int(zone_key)
            if not isinstance(zone_val, dict):
                raise ValueError(f"MSK presets YAML: region {reg_id} zone {z_id} must be a mapping")

            zones[z_id] = MSKZonePreset(
                lon_0=float(zone_val["lon_0"]),
                x_0=float(zone_val["x_0"]),
                y_0=float(zone_val["y_0"]),
            )

        out[reg_id] = MSKRegionPreset(zones=zones, gost_towgs84=gost)

    return out
