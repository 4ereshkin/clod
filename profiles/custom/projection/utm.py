from typing import Optional, Literal


class UTMBuilderParams:
    datum: Literal['WGS84', 'CGCS2000', 'PZ90', 'SK42', 'SK95']
    z_mode: Literal['ellipsoidal', 'orthometric']
    axis_order: Literal['XYZ', 'ENU', 'NED']

    utm_zone: int
    utm_hemisphere: Literal['N', 'S']

    geoid_model: Optional[str] # при z_mode = 'orthometric'

    units = 'metre'
