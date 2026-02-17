from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class CRSEpsg(BaseModel):
    crs_source: Literal['epsg']
    epsg_code: int


class CRSWkt(BaseModel):
    crs_source: Literal['wkt']
    wkt_str: str


class CRSProjJSON(BaseModel):
    crs_source: Literal['projjson']
    projjson_str: str


class CRSCustom(BaseModel):
    crs_source: Literal['custom']

    ccrs_type: Literal['latlon', 'projection']
    datum: Literal['WGS84', 'CGCS2000', 'PZ90', 'SK42', 'SK95']
    z_mode: Literal['ellipsoidal', 'orthometric']
    axis_order: Literal['XYZ', 'ENU', 'NED']

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


CRSSpec = Annotated[Union[CRSEpsg, CRSWkt, CRSProjJSON, CRSCustom], Field(discriminator='crs_source')]


class CRSNormalizeRequest(BaseModel):
    """Transport-agnostic request for CRS normalization."""

    model_config = ConfigDict(extra='forbid')

    crs: CRSSpec


class CRSNormalizeResult(BaseModel):
    model_config = ConfigDict(extra='forbid')

    payload_version: Literal['v1'] = 'v1'
    model_version: str = '1.0'

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
