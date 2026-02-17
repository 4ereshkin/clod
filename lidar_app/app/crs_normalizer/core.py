from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional, Tuple

from pyproj import CRS

from .msk_presets import MSKRegionPreset


@dataclass(frozen=True)
class NormalizedCRS:
    crs_source: Literal['epsg', 'wkt', 'projjson', 'custom']

    epsg_code: Optional[int] = None
    wkt_str: Optional[str] = None
    projjson_str: Optional[str] = None

    ccrs_type: Optional[Literal['latlon', 'projection']] = None
    datum: Optional[Literal['WGS84', 'CGCS2000', 'PZ90', 'SK42', 'SK63', 'SK95']] = None
    z_mode: Optional[Literal['ellipsoidal', 'orthometric']] = None
    axis_order: Optional[Literal['XYZ', 'ENU', 'NED']] = None
    geoid_model: Optional[str] = None

    zone_family: Optional[Literal['UTM', 'GK', 'МСК']] = None
    utm_zone: Optional[int] = None
    utm_hemisphere: Optional[Literal['N', 'S']] = None
    gk_width: Optional[Literal[3, 6]] = None
    gk_number: Optional[int] = None

    lon_0: Optional[float] = None
    lat_0: Optional[float] = None
    k0: Optional[float] = None
    x_0: Optional[float] = None
    y_0: Optional[float] = None

    msk_region: Optional[int] = None
    msk_zone: Optional[int] = None
    msk_variant: Optional[Literal['calc', 'gost']] = None
    towgs84: Optional[str] = None
    helmert_convention: Optional[Literal['position_vector']] = None

    units: Optional[Literal['metre', 'degree']] = None
    built_crs_projjson: Optional[str] = None



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



def normalize_crs_spec(crs_spec: Any, *, msk_presets: Dict[int, MSKRegionPreset]) -> NormalizedCRS:
    crs_source = getattr(crs_spec, 'crs_source', None)

    if crs_source == 'epsg':
        epsg_code = crs_spec.epsg_code
        built = CRS.from_epsg(epsg_code)
        return NormalizedCRS(crs_source='epsg', epsg_code=epsg_code, built_crs_projjson=built.to_json())

    if crs_source == 'wkt':
        wkt_str = crs_spec.wkt_str
        built = CRS.from_wkt(wkt_str)
        return NormalizedCRS(crs_source='wkt', wkt_str=wkt_str, built_crs_projjson=built.to_json())

    if crs_source == 'projjson':
        projjson_str = crs_spec.projjson_str
        built = CRS.from_json(projjson_str)
        return NormalizedCRS(crs_source='projjson', projjson_str=projjson_str, built_crs_projjson=built.to_json())

    if crs_source != 'custom':
        raise ValueError('Unknown CRS spec type')

    geoid_model = None
    if crs_spec.z_mode == 'orthometric':
        if not crs_spec.geoid_model:
            raise ValueError("z_mode='orthometric' requires geoid_model")
        geoid_model = crs_spec.geoid_model

    if crs_spec.ccrs_type == 'latlon':
        if crs_spec.datum == 'WGS84':
            built = CRS.from_epsg(4326)
        elif crs_spec.datum == 'CGCS2000':
            built = CRS.from_epsg(4490)
        elif crs_spec.datum == 'SK42':
            built = CRS.from_epsg(4284)
        else:
            raise ValueError(f'custom latlon datum={crs_spec.datum} not supported in current model without wkt/projjson')

        return NormalizedCRS(
            crs_source='custom',
            ccrs_type='latlon',
            datum=crs_spec.datum,
            z_mode=crs_spec.z_mode,
            axis_order=crs_spec.axis_order,
            geoid_model=geoid_model,
            units='degree',
            built_crs_projjson=built.to_json(),
        )

    if crs_spec.ccrs_type != 'projection':
        raise ValueError('Unknown ccrs_type')

    if crs_spec.zone_family is None:
        raise ValueError('projection requires zone_family')

    if crs_spec.zone_family == 'UTM':
        if crs_spec.datum != 'WGS84':
            raise ValueError("UTM current model supports only datum='WGS84' (EPSG:326/327)")
        if crs_spec.utm_zone is None or crs_spec.utm_hemisphere is None:
            raise ValueError('UTM requires utm_zone and utm_hemisphere')
        if not (1 <= crs_spec.utm_zone <= 60):
            raise ValueError('utm_zone must be 1..60')

        epsg = (32600 + crs_spec.utm_zone) if crs_spec.utm_hemisphere == 'N' else (32700 + crs_spec.utm_zone)
        built = CRS.from_epsg(epsg)
        return NormalizedCRS(
            crs_source='custom',
            ccrs_type='projection',
            datum=crs_spec.datum,
            z_mode=crs_spec.z_mode,
            axis_order=crs_spec.axis_order,
            geoid_model=geoid_model,
            units='metre',
            zone_family='UTM',
            utm_zone=crs_spec.utm_zone,
            utm_hemisphere=crs_spec.utm_hemisphere,
            built_crs_projjson=built.to_json(),
        )

    if crs_spec.zone_family == 'GK':
        raise ValueError('GK current model not supported yet')

    if crs_spec.zone_family != 'МСК':
        raise ValueError('Unknown zone_family')

    if crs_spec.datum != 'SK42':
        raise ValueError("МСК requires datum='SK42'")
    if crs_spec.msk_region is None or crs_spec.msk_zone is None or crs_spec.msk_variant is None:
        raise ValueError('МСК requires msk_region, msk_zone, msk_variant')

    reg = msk_presets.get(int(crs_spec.msk_region))
    if not reg:
        raise ValueError(f'No preset for MSK region {crs_spec.msk_region}')
    zone = reg.zones.get(int(crs_spec.msk_zone))
    if not zone:
        raise ValueError(f'No preset for MSK region {crs_spec.msk_region} zone {crs_spec.msk_zone}')

    lon_0 = float(crs_spec.lon_0) if crs_spec.lon_0 is not None else zone.lon_0
    x_0 = float(crs_spec.x_0) if crs_spec.x_0 is not None else zone.x_0
    y_0 = float(crs_spec.y_0) if crs_spec.y_0 is not None else zone.y_0
    lat_0 = float(crs_spec.lat_0) if crs_spec.lat_0 is not None else 0.0
    k0 = float(crs_spec.k0) if crs_spec.k0 is not None else 1.0

    projected = _build_msk_projected_projjson(lon_0=lon_0, x_0=x_0, y_0=y_0, lat_0=lat_0, k0=k0)

    towgs84 = None
    helmert = None
    final = projected

    if crs_spec.msk_variant == 'gost':
        helmert = crs_spec.helmert_convention
        if helmert != 'position_vector':
            raise ValueError("model: msk_variant='gost' requires helmert_convention='position_vector'")

        towgs84 = crs_spec.towgs84 or reg.gost_towgs84
        if not towgs84:
            raise ValueError("model: msk_variant='gost' requires towgs84 (or preset)")
        final = _wrap_boundcrs_with_towgs84(projected, towgs84)

    built = CRS.from_json(json.dumps(final))
    return NormalizedCRS(
        crs_source='custom',
        ccrs_type='projection',
        datum=crs_spec.datum,
        z_mode=crs_spec.z_mode,
        axis_order=crs_spec.axis_order,
        geoid_model=geoid_model,
        units='metre',
        zone_family='МСК',
        lon_0=lon_0,
        lat_0=lat_0,
        k0=k0,
        x_0=x_0,
        y_0=y_0,
        msk_region=int(crs_spec.msk_region),
        msk_zone=int(crs_spec.msk_zone),
        msk_variant=crs_spec.msk_variant,
        towgs84=towgs84,
        helmert_convention=helmert,
        built_crs_projjson=built.to_json(),
    )
