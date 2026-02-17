from typing import Optional, Literal
from pydantic import BaseModel, ConfigDict


class WorkflowIngestPayloadV1(BaseModel):
    model_config = ConfigDict(extra='forbid')

    payload_version: Literal['v1'] = 'v1'

    company: str
    department: str
    employee: str
    plan: Literal['free', 'admin']

    authority: Literal['prj', 'client', 'meta']

    crs_source: Literal['epsg', 'wkt', 'projjson', 'custom']

    # выбор адекватного человека, - рекомендуемые источники CRS облака
    epsg_code: Optional[int] = None
    wkt_str: Optional[str] = None
    projjson_str: Optional[str] = None

    # собираем CRS сами
    ccrs_type: Optional[Literal['latlon', 'projection']] = None
    datum: Optional[Literal['WGS84', 'CGCS2000', 'PZ90', 'SK42', 'SK63', 'SK95']] = None
    z_mode: Optional[Literal['ellipsoidal', 'orthometric']] = None
    axis_order: Optional[Literal['XYZ', 'ENU', 'NED']] = None

    # нормализованное: для ellipsoidal всегда None
    geoid_model: Optional[str] = None

    # Проекция (projection)
    zone_family: Optional[Literal['UTM', 'GK', 'МСК']] = None

    # utm
    utm_zone: Optional[int] = None
    utm_hemisphere: Optional[Literal['N', 'S']] = None

    # gk
    gk_width: Optional[Literal[3, 6]] = None
    gk_number: Optional[int] = None

    lon_0: Optional[float] = None
    lat_0: Optional[float] = None
    k0: Optional[float] = None
    x_0: Optional[float] = None
    y_0: Optional[float] = None

    # МСК
    msk_region: Optional[int] = None
    msk_zone: Optional[int] = None
    msk_variant: Optional[Literal['calc', 'gost']] = None
    towgs84: Optional[str] = None
    helmert_convention: Optional[Literal['position_vector']] = None

    # фиксируем units как итог (не “поле выбора”)
    units: Optional[Literal['metre', 'degree']] = None

    # Опционально: сохранить “готовую CRS” как projjson, чтобы реплей не зависел от pyproj/PROJ
    built_crs_projjson: Optional[str] = None
