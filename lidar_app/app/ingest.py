import json
from datetime import datetime, timezone
from pathlib import Path
import sys

# add project root to PYTHONPATH
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from .models import IngestRun, Scan, Artifact


def _deep_merge(base: dict, overrides: dict) -> dict:
    merged = dict(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _detect_point_cloud_format(raw_arts: list[Artifact]) -> str | None:
    cloud = next((a for a in raw_arts if a.kind == "raw.point_cloud"), None)
    if not cloud:
        return None
    name = (cloud.s3_key or "").lower()
    if name.endswith(".copc.laz") or "copc" in name:
        return "copc.laz"
    if name.endswith(".laz"):
        return "laz"
    if name.endswith(".las"):
        return "las"
    return None


def _linear_unit(unit: str) -> dict:
    if unit in ("m", "meter", "metre", "meters", "metres"):
        return {"type": "LinearUnit", "name": "metre", "conversion_factor": 1}
    if unit in ("km", "kilometer", "kilometre", "kilometers", "kilometres"):
        return {"type": "LinearUnit", "name": "kilometre", "conversion_factor": 1000}
    return {"type": "LinearUnit", "name": unit, "conversion_factor": 1}


def _axis_from_order(axis_order: str | None) -> list[dict]:
    if not axis_order:
        return [
            {"name": "Easting", "abbreviation": "E", "direction": "east"},
            {"name": "Northing", "abbreviation": "N", "direction": "north"},
        ]
    axes = []
    mapping = {
        "x_east": {"name": "Easting", "abbreviation": "E", "direction": "east"},
        "y_north": {"name": "Northing", "abbreviation": "N", "direction": "north"},
        "z_up": {"name": "Ellipsoidal height", "abbreviation": "h", "direction": "up"},
    }
    for entry in axis_order.split(","):
        entry = entry.strip()
        if entry in mapping:
            axes.append(mapping[entry])
    return axes or [
        {"name": "Easting", "abbreviation": "E", "direction": "east"},
        {"name": "Northing", "abbreviation": "N", "direction": "north"},
    ]


def _ellipsoid_from_name(name: str) -> dict:
    normalized = name.strip().upper().replace(" ", "").replace("_", "")
    ellipsoids = {
        "GRS80": {"name": "GRS 1980", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257222101},
        "GRS1980": {"name": "GRS 1980", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257222101},
        "CGCS2000": {"name": "CGCS2000", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257222101},
        "CGCS2000DATUM": {"name": "CGCS2000", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257222101},
        "WGS84": {"name": "WGS 84", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257223563},
        "WGS1984": {"name": "WGS 84", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257223563},
    }
    return ellipsoids.get(normalized, ellipsoids["GRS80"])


def _build_projjson(coordinate_system: dict) -> dict | None:
    projection = coordinate_system.get("projection") or {}
    projection_type = projection.get("type")
    central_meridian = projection.get("central_meridian")
    zone_width = projection.get("zone_width")
    zone_number = projection.get("zone_number")

    if projection_type is None and central_meridian is None and zone_width is None:
        return None

    method = "Transverse Mercator"
    if projection_type not in (None, "GK", "MCK", "tmerc"):
        method = str(projection_type)

    lon_0 = central_meridian
    if lon_0 is None and zone_width is not None and zone_number is not None:
        lon_0 = (zone_width * zone_number) - (zone_width / 2)
    if lon_0 is None:
        return None

    lat_0 = projection.get("lat_0", 0)
    k = projection.get("k", 1)
    x_0 = projection.get("x_0", 500000)
    y_0 = projection.get("y_0", 0)
    datum = coordinate_system.get("datum") or projection.get("ellps") or "GRS80"
    units = coordinate_system.get("units") or "m"
    ellipsoid = _ellipsoid_from_name(datum)

    return {
        "type": "ProjectedCRS",
        "name": coordinate_system.get("name") or f"{datum} / {method}",
        "base_crs": {
            "type": "GeographicCRS",
            "name": datum,
            "datum": {
                "type": "GeodeticReferenceFrame",
                "name": datum,
                "ellipsoid": ellipsoid,
            },
            "coordinate_system": {
                "subtype": "ellipsoidal",
                "axis": [
                    {"name": "Latitude", "abbreviation": "Lat", "direction": "north"},
                    {"name": "Longitude", "abbreviation": "Lon", "direction": "east"},
                ],
                "unit": {"type": "AngularUnit", "name": "degree", "conversion_factor": 0.0174532925199433},
            },
        },
        "conversion": {
            "name": f"{method} conversion",
            "method": {"name": method},
            "parameters": [
                {
                    "name": "Latitude of natural origin",
                    "value": lat_0,
                    "unit": {"type": "AngularUnit", "name": "degree", "conversion_factor": 0.0174532925199433},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": lon_0,
                    "unit": {"type": "AngularUnit", "name": "degree", "conversion_factor": 0.0174532925199433},
                },
                {"name": "Scale factor at natural origin", "value": k, "unit": {"type": "ScaleUnit", "name": "unity", "conversion_factor": 1}},
                {"name": "False easting", "value": x_0, "unit": _linear_unit(units)},
                {"name": "False northing", "value": y_0, "unit": _linear_unit(units)},
            ],
        },
        "coordinate_system": {
            "subtype": "Cartesian",
            "axis": _axis_from_order(coordinate_system.get("axis_order")),
            "unit": _linear_unit(units),
        },
    }


def _apply_control_point_defaults(manifest: dict) -> None:
    control_points = manifest.get("control_points") or {}
    verified = control_points.get("verified_from_control_point") or {}
    verified_cs = verified.get("coordinate_system") or {}

    coordinate_system = manifest.setdefault("coordinate_system", {})
    for key in ("crs_type", "datum", "units", "axis_order"):
        if coordinate_system.get(key) is None and verified_cs.get(key) is not None:
            coordinate_system[key] = verified_cs[key]

    projection = coordinate_system.setdefault("projection", {})
    verified_projection = verified_cs.get("projection") or {}
    for key in ("type", "zone_width", "zone_number", "central_meridian"):
        if projection.get(key) is None and verified_projection.get(key) is not None:
            projection[key] = verified_projection[key]

    if manifest.get("geometry_mode") is None and verified.get("geometry_mode") is not None:
        manifest["geometry_mode"] = verified["geometry_mode"]
    if manifest.get("z_measurement") is None and verified.get("z_measurement") is not None:
        manifest["z_measurement"] = verified["z_measurement"]


def build_ingest_manifest(*, run: IngestRun, scan: Scan, raw_arts: list[Artifact]) -> dict:
    def a_to_dict(a: Artifact) -> dict:
        return {
            "kind": a.kind,
            "bucket": a.s3_bucket,
            "key": a.s3_key,
            "etag": a.etag,
            "size_bytes": a.size_bytes,
            "status": a.status,
            "meta": a.meta or {},
        }

    def _artifact_by_kind(kind: str) -> dict | None:
        art = next((a for a in raw_arts if a.kind == kind), None)
        if not art:
            return None
        return a_to_dict(art)

    scan_meta = scan.meta or {}
    overrides = scan_meta.get("manifest", {}) if isinstance(scan_meta.get("manifest"), dict) else {}

    manifest = {
        "material": {
            "point_cloud_format": _detect_point_cloud_format(raw_arts),
        },
        "coordinate_system": {
            "guarantor": {
                "source": None,
                "metadata": None,
                "reference": None,
                "client": None,
            },
            "crs_id": scan.crs_id,
            "crs_type": None,
            "datum": None,
            "projection": {
                "type": None,
                "zone_width": None,
                "zone_number": None,
                "central_meridian": None,
            },
            "units": None,
            "axis_order": None,
        },
        "z_measurement": None,
        "imu_dimensions": None,
        "geometry_mode": None,
        "control_points": {
            "table": _artifact_by_kind("raw.control_point"),
            "local_system": {
                "x": None,
                "y": None,
                "z": None,
                "z_mode": None,
            },
            "final_system": {
                "x": None,
                "y": None,
                "z": None,
                "z_mode": None,
            },
            "verified_from_control_point": {
                "who_guarantees": None,
                "xyz_consistent": None,
                "geometry_mode": None,
                "z_measurement": None,
                "gps": {
                    "latlon_format": None,
                    "height_type": None,
                },
                "coordinate_system": {
                    "crs_type": None,
                    "datum": None,
                    "projection": {
                        "type": None,
                        "zone_width": None,
                        "zone_number": None,
                        "central_meridian": None,
                    },
                    "units": None,
                    "axis_order": None,
                },
            },
        },
        "business_logic": {
            "company": scan.company_id,
            "department": scan.owner_department_id,
            "employee": None,
            "tariff": None,
            "processing_version": scan.schema_version,
        },
        "recording_modes": {
            "duplicates": None,
            "coordinate_system": {
                "mode": None,
                "override_epsg": None,
            },
        },
        "ingest": {
            "run_id": int(run.id),
            "company_id": run.company_id,
            "scan_id": run.scan_id,
            "schema_version": run.schema_version,
            "input_fingerprint": run.input_fingerprint,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "scan": {
                "id": scan.id,
                "dataset_id": scan.dataset_id,
                "dataset_version_id": scan.dataset_version_id,
                "crs_id": scan.crs_id,
                "status": scan.status,
                "schema_version": scan.schema_version,
                "meta": scan.meta or {},
            },
            "raw_artifacts": [a_to_dict(a) for a in raw_arts],
        },
    }

    merged = _deep_merge(manifest, overrides)
    _apply_control_point_defaults(merged)
    if "projjson" not in (merged.get("coordinate_system") or {}):
        merged.setdefault("coordinate_system", {})["projjson"] = _build_projjson(
            merged.get("coordinate_system") or {}
        )

    return merged
