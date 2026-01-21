from typing import Optional, Literal


class UTMBuilderParams:
    datum: Literal['WGS84', 'CGCS2000', 'PZ90', 'SK42', 'SK95']
    z_mode: Literal['ellipsoidal', 'orthometric']
    axis_order: Literal['XYZ', 'ENU', 'NED']

    gk_width: Literal[3, 6]
    gk_number: Optional[int]

    lon_0: Optional[float]
    lat_0: Optional[float]

    x_0: Optional[float]
    y_0: Optional[float]

    k0: Optional[float]

    geoid_model: str # при z_mode = 'orthometric'

    units = 'metre'
