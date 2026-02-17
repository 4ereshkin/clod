from __future__ import annotations

import json
from typing import Dict, Tuple

from pyproj import CRS

from .models_v1 import CRSNormalizeRequestV1, CRSNormalizeResultV1, CRSCustom, CRSEpsg, CRSProjJSON, CRSWkt
from .msk_presets import MSKRegionPreset


def _parse_towgs84_7(s: str) -> Tuple[float, float, float, float, float, float, float]:
    parts = [p.strip() for p in s.split(',')]
    if len(parts) != 7:
        raise ValueError('towgs84 must contain 7 numbers: dx,dy,dz,rx,ry,rz,ds')
    vals = [float(x) for x in parts]
    return vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6]


def _build_msk_projected_projjson(*, lon_0: float, x_0: float, y_0: float, lat_0: float, k0: float) -> dict:
    base = CRS.from_epsg(4284)
    return {
        'type': 'ProjectedCRS',
        'name': 'MSK (custom, SK42/Krassovsky)',
        'base_crs': json.loads(base.to_json()),
        'conversion': {
            'type': 'Conversion',
            'name': 'Transverse Mercator',
            'method': {'name': 'Transverse Mercator', 'id': {'authority': 'EPSG', 'code': 9807}},
            'parameters': [
                {'name': 'Latitude of natural origin', 'value': float(lat_0), 'unit': {'type': 'AngularUnit', 'name': 'degree', 'conversion_factor': 0.0174532925199433}, 'id': {'authority': 'EPSG', 'code': 8801}},
                {'name': 'Longitude of natural origin', 'value': float(lon_0), 'unit': {'type': 'AngularUnit', 'name': 'degree', 'conversion_factor': 0.0174532925199433}, 'id': {'authority': 'EPSG', 'code': 8802}},
                {'name': 'Scale factor at natural origin', 'value': float(k0), 'unit': {'type': 'ScaleUnit', 'name': 'unity', 'conversion_factor': 1.0}, 'id': {'authority': 'EPSG', 'code': 8805}},
                {'name': 'False easting', 'value': float(x_0), 'unit': {'type': 'LinearUnit', 'name': 'metre', 'conversion_factor': 1.0}, 'id': {'authority': 'EPSG', 'code': 8806}},
                {'name': 'False northing', 'value': float(y_0), 'unit': {'type': 'LinearUnit', 'name': 'metre', 'conversion_factor': 1.0}, 'id': {'authority': 'EPSG', 'code': 8807}},
            ],
        },
        'coordinate_system': {
            'type': 'CartesianCS',
            'subtype': 'plane',
            'axis': [
                {'name': 'Easting', 'abbreviation': 'E', 'direction': 'east', 'unit': {'type': 'LinearUnit', 'name': 'metre', 'conversion_factor': 1.0}},
                {'name': 'Northing', 'abbreviation': 'N', 'direction': 'north', 'unit': {'type': 'LinearUnit', 'name': 'metre', 'conversion_factor': 1.0}},
            ],
        },
    }


def _wrap_boundcrs_with_towgs84(projected: dict, towgs84: str) -> dict:
    dx, dy, dz, rx, ry, rz, ds = _parse_towgs84_7(towgs84)
    return {
        'type': 'BoundCRS',
        'source_crs': projected,
        'target_crs': json.loads(CRS.from_epsg(4326).to_json()),
        'transformation': {
            'type': 'Transformation',
            'name': 'towgs84 (7-parameter Helmert)',
            'method': {'name': 'Position Vector transformation (geocentric domain)', 'id': {'authority': 'EPSG', 'code': 1033}},
            'parameters': [
                {'name': 'X-axis translation', 'value': dx, 'unit': {'type': 'LinearUnit', 'name': 'metre', 'conversion_factor': 1.0}},
                {'name': 'Y-axis translation', 'value': dy, 'unit': {'type': 'LinearUnit', 'name': 'metre', 'conversion_factor': 1.0}},
                {'name': 'Z-axis translation', 'value': dz, 'unit': {'type': 'LinearUnit', 'name': 'metre', 'conversion_factor': 1.0}},
                {'name': 'X-axis rotation', 'value': rx, 'unit': {'type': 'AngularUnit', 'name': 'arc-second', 'conversion_factor': 4.84813681109536e-06}},
                {'name': 'Y-axis rotation', 'value': ry, 'unit': {'type': 'AngularUnit', 'name': 'arc-second', 'conversion_factor': 4.84813681109536e-06}},
                {'name': 'Z-axis rotation', 'value': rz, 'unit': {'type': 'AngularUnit', 'name': 'arc-second', 'conversion_factor': 4.84813681109536e-06}},
                {'name': 'Scale difference', 'value': ds, 'unit': {'type': 'ScaleUnit', 'name': 'parts per million', 'conversion_factor': 1e-06}},
            ],
        },
    }


def normalize_crs_v1(req: CRSNormalizeRequestV1, *, msk_presets: Dict[int, MSKRegionPreset]) -> CRSNormalizeResultV1:
    crs_spec = req.crs

    if isinstance(crs_spec, CRSEpsg):
        built = CRS.from_epsg(crs_spec.epsg_code)
        return CRSNormalizeResultV1(payload_version='v1', crs_source='epsg', epsg_code=crs_spec.epsg_code, built_crs_projjson=built.to_json())

    if isinstance(crs_spec, CRSWkt):
        built = CRS.from_wkt(crs_spec.wkt_str)
        return CRSNormalizeResultV1(payload_version='v1', crs_source='wkt', wkt_str=crs_spec.wkt_str, built_crs_projjson=built.to_json())

    if isinstance(crs_spec, CRSProjJSON):
        built = CRS.from_json(crs_spec.projjson_str)
        return CRSNormalizeResultV1(payload_version='v1', crs_source='projjson', projjson_str=crs_spec.projjson_str, built_crs_projjson=built.to_json())

    if not isinstance(crs_spec, CRSCustom):
        raise ValueError('Unknown CRS spec type')

    c = crs_spec
    geoid_model = None
    if c.z_mode == 'orthometric':
        if not c.geoid_model:
            raise ValueError("z_mode='orthometric' requires geoid_model")
        geoid_model = c.geoid_model

    if c.ccrs_type == 'latlon':
        units = 'degree'
        if c.datum == 'WGS84':
            built = CRS.from_epsg(4326)
        elif c.datum == 'CGCS2000':
            built = CRS.from_epsg(4490)
        elif c.datum == 'SK42':
            built = CRS.from_epsg(4284)
        else:
            raise ValueError(f'custom latlon datum={c.datum} not supported in V1 without wkt/projjson')

        return CRSNormalizeResultV1(payload_version='v1', crs_source='custom', ccrs_type='latlon', datum=c.datum, z_mode=c.z_mode, axis_order=c.axis_order, geoid_model=geoid_model, units=units, built_crs_projjson=built.to_json())

    if c.ccrs_type != 'projection':
        raise ValueError('Unknown ccrs_type')

    units = 'metre'
    if c.zone_family is None:
        raise ValueError('projection requires zone_family')

    if c.zone_family == 'UTM':
        if c.datum != 'WGS84':
            raise ValueError("UTM V1 supports only datum='WGS84' (EPSG:326/327)")
        if c.utm_zone is None or c.utm_hemisphere is None:
            raise ValueError('UTM requires utm_zone and utm_hemisphere')
        if not (1 <= c.utm_zone <= 60):
            raise ValueError('utm_zone must be 1..60')

        epsg = (32600 + c.utm_zone) if c.utm_hemisphere == 'N' else (32700 + c.utm_zone)
        built = CRS.from_epsg(epsg)
        return CRSNormalizeResultV1(payload_version='v1', crs_source='custom', ccrs_type='projection', datum=c.datum, z_mode=c.z_mode, axis_order=c.axis_order, geoid_model=geoid_model, units=units, zone_family='UTM', utm_zone=c.utm_zone, utm_hemisphere=c.utm_hemisphere, built_crs_projjson=built.to_json())

    if c.zone_family == 'GK':
        raise ValueError('GK V1 not supported yet')

    if c.zone_family == 'МСК':
        if c.datum != 'SK42':
            raise ValueError("МСК requires datum='SK42'")
        if c.msk_region is None or c.msk_zone is None or c.msk_variant is None:
            raise ValueError('МСК requires msk_region, msk_zone, msk_variant')

        reg = msk_presets.get(int(c.msk_region))
        if not reg:
            raise ValueError(f'No preset for MSK region {c.msk_region}')
        zone = reg.zones.get(int(c.msk_zone))
        if not zone:
            raise ValueError(f'No preset for MSK region {c.msk_region} zone {c.msk_zone}')

        lon_0 = float(c.lon_0) if c.lon_0 is not None else zone.lon_0
        x_0 = float(c.x_0) if c.x_0 is not None else zone.x_0
        y_0 = float(c.y_0) if c.y_0 is not None else zone.y_0
        lat_0 = float(c.lat_0) if c.lat_0 is not None else 0.0
        k0 = float(c.k0) if c.k0 is not None else 1.0

        projected = _build_msk_projected_projjson(lon_0=lon_0, x_0=x_0, y_0=y_0, lat_0=lat_0, k0=k0)

        towgs84 = None
        helmert = None
        final = projected

        if c.msk_variant == 'gost':
            helmert = c.helmert_convention
            if helmert != 'position_vector':
                raise ValueError("V1: msk_variant='gost' requires helmert_convention='position_vector'")
            towgs84 = c.towgs84 or reg.gost_towgs84
            if not towgs84:
                raise ValueError("V1: msk_variant='gost' requires towgs84 (or preset)")
            final = _wrap_boundcrs_with_towgs84(projected, towgs84)

        built = CRS.from_json(json.dumps(final))
        return CRSNormalizeResultV1(payload_version='v1', crs_source='custom', ccrs_type='projection', datum=c.datum, z_mode=c.z_mode, axis_order=c.axis_order, geoid_model=geoid_model, units=units, zone_family='МСК', lon_0=lon_0, lat_0=lat_0, k0=k0, x_0=x_0, y_0=y_0, msk_region=int(c.msk_region), msk_zone=int(c.msk_zone), msk_variant=c.msk_variant, towgs84=towgs84, helmert_convention=helmert, built_crs_projjson=built.to_json())

    raise ValueError('Unknown zone_family')
