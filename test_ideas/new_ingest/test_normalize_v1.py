# test_normalize_v1.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import pytest
from pyproj import CRS

from ingest_request import IngestRequest
from msk_presets import load_msk_presets_yaml
from normalize_to_workflow_v1 import normalize_to_workflow_v1


def _write_msk_yaml(tmp_path: Path) -> Path:
    p = tmp_path / "msk_presets.yml"
    p.write_text(
        """\
region:
  '66': # МСК-66 Свердловская область
    '1':
      lon_0: 60.05
      x_0: 1500000.0
      y_0: -5911057.63
    '2':
      lon_0: 63.05
      x_0: 2500000.0
      y_0: -5911057.63
    '3':
      lon_0: 66.05
      x_0: 3500000.0
      y_0: -5911057.63
    gost_towgs84: "23.57,-140.95,-79.8,0,0.35,0.79,-0.22"
""",
        encoding="utf-8",
    )
    return p


def _base_request(crs: dict) -> dict:
    return {
        "company": "acme",
        "department": "geo",
        "employee": "ivan",
        "plan": "free",
        "schema_version": "v1",
        "authority": "client",
        "crs": crs,
    }


def test_epsg_ok(tmp_path: Path):
    presets_path = _write_msk_yaml(tmp_path)
    presets = load_msk_presets_yaml(str(presets_path))

    raw = _base_request({"crs_source": "epsg", "epsg_code": 32637})
    req = IngestRequest.model_validate(raw)

    payload = normalize_to_workflow_v1(req, msk_presets=presets)

    assert payload.crs_source == "epsg"
    assert payload.epsg_code == 32637
    assert payload.built_crs_projjson

    crs = CRS.from_json(payload.built_crs_projjson)
    assert crs.to_epsg() == 32637


def test_wkt_ok(tmp_path: Path):
    presets_path = _write_msk_yaml(tmp_path)
    presets = load_msk_presets_yaml(str(presets_path))

    wkt = CRS.from_epsg(4326).to_wkt()  # детерминированно генерируем в тесте
    raw = _base_request({"crs_source": "wkt", "wkt_str": wkt})
    req = IngestRequest.model_validate(raw)

    payload = normalize_to_workflow_v1(req, msk_presets=presets)

    assert payload.crs_source == "wkt"
    assert payload.wkt_str
    assert payload.built_crs_projjson

    crs = CRS.from_json(payload.built_crs_projjson)
    assert crs.to_epsg() == 4326


def test_projjson_ok(tmp_path: Path):
    presets_path = _write_msk_yaml(tmp_path)
    presets = load_msk_presets_yaml(str(presets_path))

    pj = CRS.from_epsg(3857).to_json()
    raw = _base_request({"crs_source": "projjson", "projjson_str": pj})
    req = IngestRequest.model_validate(raw)

    payload = normalize_to_workflow_v1(req, msk_presets=presets)

    assert payload.crs_source == "projjson"
    assert payload.projjson_str
    assert payload.built_crs_projjson

    crs = CRS.from_json(payload.built_crs_projjson)
    assert crs.to_epsg() == 3857


def test_custom_latlon_wgs84_ellipsoidal_ok(tmp_path: Path):
    presets_path = _write_msk_yaml(tmp_path)
    presets = load_msk_presets_yaml(str(presets_path))

    raw = _base_request(
        {
            "crs_source": "custom",
            "ccrs_type": "latlon",
            "datum": "WGS84",
            "z_mode": "ellipsoidal",
            "axis_order": "ENU",
            # geoid_model omitted ok
        }
    )
    req = IngestRequest.model_validate(raw)
    payload = normalize_to_workflow_v1(req, msk_presets=presets)

    assert payload.crs_source == "custom"
    assert payload.ccrs_type == "latlon"
    assert payload.units == "degree"
    assert payload.geoid_model is None
    assert payload.built_crs_projjson

    crs = CRS.from_json(payload.built_crs_projjson)
    assert crs.to_epsg() == 4326


def test_custom_utm_ok(tmp_path: Path):
    presets_path = _write_msk_yaml(tmp_path)
    presets = load_msk_presets_yaml(str(presets_path))

    raw = _base_request(
        {
            "crs_source": "custom",
            "ccrs_type": "projection",
            "datum": "WGS84",
            "z_mode": "ellipsoidal",
            "axis_order": "ENU",
            "zone_family": "UTM",
            "utm_zone": 37,
            "utm_hemisphere": "N",
        }
    )
    req = IngestRequest.model_validate(raw)
    payload = normalize_to_workflow_v1(req, msk_presets=presets)

    assert payload.zone_family == "UTM"
    assert payload.units == "metre"
    assert payload.utm_zone == 37
    assert payload.utm_hemisphere == "N"
    assert payload.built_crs_projjson

    crs = CRS.from_json(payload.built_crs_projjson)
    assert crs.to_epsg() == 32637


def test_custom_msk_calc_autofill_ok(tmp_path: Path):
    presets_path = _write_msk_yaml(tmp_path)
    presets = load_msk_presets_yaml(str(presets_path))

    raw = _base_request(
        {
            "crs_source": "custom",
            "ccrs_type": "projection",
            "datum": "SK42",
            "z_mode": "ellipsoidal",
            "axis_order": "ENU",
            "zone_family": "МСК",
            "msk_region": 66,
            "msk_zone": 1,
            "msk_variant": "calc",
            # lon_0/x_0/y_0 omitted -> autofill from YAML
        }
    )
    req = IngestRequest.model_validate(raw)
    payload = normalize_to_workflow_v1(req, msk_presets=presets)

    assert payload.zone_family == "МСК"
    assert payload.msk_variant == "calc"
    assert payload.lon_0 == pytest.approx(60.05)
    assert payload.x_0 == pytest.approx(1500000.0)
    assert payload.y_0 == pytest.approx(-5911057.63)
    assert payload.k0 == pytest.approx(1.0)
    assert payload.lat_0 == pytest.approx(0.0)
    assert payload.towgs84 is None
    assert payload.helmert_convention is None


def test_custom_msk_gost_uses_preset_towgs84(tmp_path: Path):
    presets_path = _write_msk_yaml(tmp_path)
    presets = load_msk_presets_yaml(str(presets_path))

    raw = _base_request(
        {
            "crs_source": "custom",
            "ccrs_type": "projection",
            "datum": "SK42",
            "z_mode": "ellipsoidal",
            "axis_order": "ENU",
            "zone_family": "МСК",
            "msk_region": 66,
            "msk_zone": 1,
            "msk_variant": "gost",
            "helmert_convention": "position_vector",
            # towgs84 omitted -> should come from preset
        }
    )
    req = IngestRequest.model_validate(raw)
    payload = normalize_to_workflow_v1(req, msk_presets=presets)

    assert payload.msk_variant == "gost"
    assert payload.helmert_convention == "position_vector"
    assert payload.towgs84 == "23.57,-140.95,-79.8,0,0.35,0.79,-0.22"
    assert payload.built_crs_projjson  # если в normalize_v1 ты строишь CRS.from_json(bound)
