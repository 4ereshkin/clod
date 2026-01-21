from typing import Literal


class LatLonBuilderParams:
    datum: Literal['WGS84', 'CGCS2000', 'PZ90', 'SK42', 'SK95']
    z_mode: Literal['ellipsoidal', 'orthometric']
    axis_order: Literal['XYZ', 'ENU', 'NED']

    geoid_model: str # при z_mode = 'orthometric'

    units = 'degree'