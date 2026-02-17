from typing import Annotated, Literal, Union, Optional
from pydantic import BaseModel, Field, ConfigDict

class CRSEpsg(BaseModel):
    crs_source: Literal['epsg']
    epsg_code: int

class CRSWkt(BaseModel):
    crs_source: Literal['wkt']
    wkt_str: str

class CRSProjJSON(BaseModel):
    crs_source: Literal['projjson']
    projjson_str: str

# custom пока можно держать “плоским” (минимум), а потом распилить дальше
class CRSCustom(BaseModel):
    crs_source: Literal['custom']

    ccrs_type: Literal['latlon', 'projection']
    datum: Literal['WGS84', 'CGCS2000', 'PZ90', 'SK42', 'SK95']
    z_mode: Literal['ellipsoidal', 'orthometric']
    axis_order: Literal['XYZ', 'ENU', 'NED']

    geoid_model: Optional[str] = None

    # дальше поля для projection/семейств (пока optional, правила будут в normalize)
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


CRSSpec = Annotated[
    Union[CRSEpsg, CRSWkt, CRSProjJSON, CRSCustom],
    Field(discriminator='crs_source')
]

class IngestRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    company: str
    department: str
    employee: str
    plan: Literal['free', 'admin']

    schema_version: str
    authority: Literal['prj', 'client', 'meta']

    crs: CRSSpec
