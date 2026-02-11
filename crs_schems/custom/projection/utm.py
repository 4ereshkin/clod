from typing import Annotated, Union, Literal

from pydantic import BaseModel, Field

Datum = Literal['WGS84', 'CGCS2000', 'PZ90', 'SK42', 'SK95']
AxisOrder = Literal['XYZ', 'ENU', 'NED']

class UTMBase(BaseModel):

    datum: Datum
    axis_order: AxisOrder

    utm_zone: int
    utm_hemisphere: Literal['N', 'S']
    units: Literal['metre'] = 'metre'

class UTMOrthometric(UTMBase):
    z_mode: Literal['orthometric']
    geoid_model: str

class UTMEllipsoidal(UTMBase):
    z_mode: Literal['ellipsoidal']
    geoid_model: Literal[None] = None


UTMBuilderParams = Annotated[
    Union[UTMOrthometric, UTMEllipsoidal],
    Field(discriminator='z_mode')]