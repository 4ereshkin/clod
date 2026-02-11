from typing import Union, Literal, Annotated
from pydantic import BaseModel, Field

Datum = Literal['WGS84', 'CGCS2000', 'PZ90', 'SK42', 'SK95']
AxisOrder = Literal['XYZ', 'ENU', 'NED']

class LatLonBase(BaseModel):
    datum: Datum
    axis_order: AxisOrder

    units: Literal['degree'] = 'degree'

class LatLonOrthometric(LatLonBase):
    z_mode: Literal['orthometric']
    geoid_model: str

class LatLonEllipsoidal(LatLonBase):
    z_mode: Literal['ellipsoidal']
    geoid_model: Literal[None] = None


LatLonBuilderParams = Annotated[
    Union[LatLonOrthometric, LatLonEllipsoidal],
    Field(discriminator='z_mode')]