from typing import Union, Optional, Literal, Annotated
from pydantic import BaseModel, Field

Datum = Literal['WGS84', 'CGCS2000', 'PZ90', 'SK42', 'SK95']
AxisOrder = Literal['XYZ', 'ENU', 'NED']

class GKBase(BaseModel):
    datum: Datum
    axis_order: AxisOrder

    gk_width: Literal[3, 6]
    gk_number: int

    lon_0: Optional[float]
    lat_0: Optional[float]

    x_0: Optional[float]
    y_0: Optional[float]

    k0: Optional[float]

    units: Literal['metre'] = 'metre'

class GKOrthometric(GKBase):
    z_mode: Literal['orthometric']
    geoid_model: str

class GKEllipsoidal(GKBase):
    z_mode: Literal['ellipsoidal']
    geoid_model: Literal[None] = None


GkBuilderParams = Annotated[
    Union[GKOrthometric, GKEllipsoidal],
    Field(discriminator='z_mode')]