from dotenv import dotenv_values

from typing import Dict, Literal, Optional
from dataclasses import dataclass
from pydantic import BaseModel, model_validator

from temporalio import workflow
from temporalio.exceptions import ApplicationError

from pyproj import CRS
import json


# 0. Предварительная конфигурация
def env_values(env_file: str) -> Dict[str, str] | None:
    if not isinstance(env_file, str):
        raise ApplicationError('Некорректная передача .env файла. Попробуйте str формат.')
    return dotenv_values(
                        dotenv_path=env_file,
                        encoding='utf-8',)

env = env_values('.env')
WORKFLOW_VERSION = env['WORKFLOW_VERSION']
SCHEMA_VERSION = env['SCHEMA_VERSION']

# 1. Конфигурация воркфлоу

def parse_workflow_config(config_path: str) -> Dict[str, str] | None:
    if not isinstance(config_path, str):
        raise ApplicationError('Некорректная передача конфига для воркфлоу Попробуйте str формат')

MSK_PRESETS = {
    66: {  # МСК-66
        1: {"lon_0": 60.05, "x_0": 1500000.0, "y_0": -5911057.63},
        2: {"lon_0": 63.05, "x_0": 2500000.0, "y_0": -5911057.63},
        3: {"lon_0": 66.05, "x_0": 3500000.0, "y_0": -5911057.63},
        "gost_towgs84": "23.57,-140.95,-79.8,0,0.35,0.79,-0.22",
    }
}


class IngestParams(BaseModel):
    # Пользователь обработчика
    company: str
    department: str
    employee: str
    plan: Literal['free', 'admin']

    schema_version: str

    authority: Literal['prj', 'client', 'meta']
    crs_source: Literal['epsg', 'wkt', 'projjson', 'custom']

    # custom-only (пока оставляем как есть)
    ccrs_type: Optional[Literal['latlon', 'projection']] = None
    datum: Optional[Literal['WGS84', 'CGCS2000', 'PZ90', 'SK42', 'SK95']] = None
    z_mode: Optional[Literal['ellipsoidal', 'orthometric']] = None
    axis_order: Optional[Literal['XYZ', 'ENU', 'NED']] = None
    zone_family: Optional[Literal['UTM', 'GK', 'МСК']] = None
    utm_zone: Optional[int] = None
    utm_hemisphere: Optional[Literal['N', 'S']] = None
    gk_width: Optional[Literal[3, 6]] = None
    gk_number: Optional[int] = None
    units: Optional[Literal['metre', 'degree']] = None
    lon_0: Optional[float] = None # центральный меридиан в градусах
    lat_0: Optional[float] = None
    k0: Optional[float] = None
    x_0: Optional[float] = None # false easting, метры
    y_0: Optional[float] = None # false northing, метры

    msk_region: Optional[int] = None  # например 66
    msk_zone: Optional[int] = None  # 1..3 для МСК-66
    msk_variant: Optional[Literal["calc", "gost"]] = None  # расчетные или ГОСТ
    towgs84: Optional[str] = None  # "dx,dy,dz,rx,ry,rz,ds"

    # non-custom sources
    epsg_code: Optional[int] = None
    wkt_str: Optional[str] = None
    projjson_str: Optional[str] = None

    geoid_model: Optional[str] = None

    helmert_convention: Optional[Literal["position_vector"]] = None

    @model_validator(mode='after')
    def _validate_crs_source(self):

        def _assert_no_custom_fields():
            custom_fields = [
                self.ccrs_type, self.datum, self.z_mode, self.axis_order, self.zone_family,
                self.utm_zone, self.utm_hemisphere, self.gk_width, self.gk_number, self.units,
                self.lon_0, self.lat_0, self.k0, self.x_0, self.y_0,
                self.msk_region, self.msk_zone, self.msk_variant, self.towgs84,
                self.geoid_model, self.helmert_convention
            ]
            if any(v is not None for v in custom_fields):
                raise ValueError(f"режим {self.crs_source.upper()} запрещает custom CRS поля")

        def _sanity():
            try:
                self.build_pyproj_crs()
            except Exception as e:
                raise ValueError(f"CRS не собирается через pyproj: {e}") from e

        src = self.crs_source

        if src == 'epsg':
            if self.epsg_code is None:
                raise ValueError("crs_source='epsg' требует указания epsg_code")
            if self.wkt_str is not None or self.projjson_str is not None:
                raise ValueError("режим EPSG запрещает wkt_str/projjson_str")

            _assert_no_custom_fields()
            _sanity()
            return self

        if src == 'wkt':
            if not self.wkt_str:
                raise ValueError("crs_source='wkt' требует wkt_str")
            if self.epsg_code is not None or self.projjson_str is not None:
                raise ValueError("режим WKT запрещает epsg_code/projjson_str")

            _assert_no_custom_fields()
            _sanity()
            return self

        if src == 'projjson':
            if not self.projjson_str:
                raise ValueError("crs_source='projjson' требует projjson_str")
            if self.epsg_code is not None or self.wkt_str is not None:
                raise ValueError("режим PROJJSON запрещает epsg_code/wkt_str")

            _assert_no_custom_fields()
            _sanity()
            return self

        if src == 'custom':
            # обязательные базовые поля
            required = {
                "ccrs_type": self.ccrs_type,
                "datum": self.datum,
                "z_mode": self.z_mode,
                "axis_order": self.axis_order,
                "units": self.units,
            }
            missing = [k for k, v in required.items() if v is None]
            if missing:
                raise ValueError(f"crs_source='custom' требует поля: {', '.join(missing)}")

            # z_mode=orthometric -> geoid_model
            if self.z_mode == 'orthometric' and not self.geoid_model:
                raise ValueError("z_mode='orthometric' требует параметра geoid_model")

            # units согласуются с типом
            if self.ccrs_type == 'latlon':
                if self.units != 'degree':
                    raise ValueError("custom latlon требует units='degree'")
                # для latlon запрещаем проекционные поля
                if any(v is not None for v in
                       [self.zone_family, self.utm_zone, self.utm_hemisphere, self.gk_width, self.gk_number]):
                    raise ValueError("latlon запрещает поля проекции (zone_family/utm_*/gk_*)")
                _sanity()
                return self

            if self.ccrs_type == 'projection':
                if self.units != 'metre':
                    raise ValueError("custom projection требует units='metre'")
                if self.zone_family is None:
                    raise ValueError("projection требует zone_family")

                if self.zone_family == 'UTM':
                    if self.utm_zone is None or self.utm_hemisphere is None:
                        raise ValueError("UTM требует utm_zone и utm_hemisphere")
                    if not (1 <= self.utm_zone <= 60):
                        raise ValueError("utm_zone должен быть в диапазоне 1..60")
                    # запрещаем GK поля
                    if self.gk_width is not None or self.gk_number is not None:
                        raise ValueError("UTM запрещает gk_width/gk_number")
                    _sanity()
                    return self

                if self.zone_family == 'GK':
                    raise ValueError("GK пока не поддержан: сборка CRS не реализована (нужны правила lon_0/x_0/y_0)")

                if self.zone_family == 'МСК':
                    if self.datum != 'SK42':
                        raise ValueError("МСК (Красовский) требует datum='SK42'")

                    # 1) требуем идентификаторы
                    if self.msk_region is None or self.msk_zone is None:
                        raise ValueError("МСК требует msk_region и msk_zone (например 66 и 1/2/3)")
                    if self.msk_variant is None:
                        raise ValueError("МСК требует msk_variant: 'calc' или 'gost'")

                    # 2) подтягиваем пресет
                    reg = MSK_PRESETS.get(self.msk_region)
                    if not reg:
                        raise ValueError(f"Нет пресета для МСК-{self.msk_region}")

                    zone = reg.get(self.msk_zone)
                    if not zone:
                        raise ValueError(f"Нет пресета для МСК-{self.msk_region} зона {self.msk_zone}")

                    # 3) автозаполнение, если не задано руками
                    self.lon_0 = self.lon_0 if self.lon_0 is not None else float(zone["lon_0"])
                    self.x_0 = self.x_0 if self.x_0 is not None else float(zone["x_0"])
                    self.y_0 = self.y_0 if self.y_0 is not None else float(zone["y_0"])

                    self.lat_0 = self.lat_0 if self.lat_0 is not None else 0.0
                    self.k0 = self.k0 if self.k0 is not None else 1.0

                    # 4) towgs84 — только если вариант ГОСТ
                    if self.msk_variant == "gost":
                        if self.helmert_convention is None:
                            # можно и автозаполнить, но лучше заставить явно
                            raise ValueError("msk_variant='gost' требует helmert_convention='position_vector'")
                        if self.helmert_convention != "position_vector":
                            raise ValueError(
                                "Поддерживается только helmert_convention='position_vector' для msk_variant='gost'")

                        self.towgs84 = self.towgs84 if self.towgs84 is not None else reg.get("gost_towgs84")
                        if not self.towgs84:
                            raise ValueError("msk_variant='gost' требует towgs84 (или пресет должен его содержать)")

                    _sanity()

                    return self

        return self

    def build_pyproj_crs(self) -> CRS:
        # 1) Прямые источники CRS
        if self.crs_source == "epsg":
            return CRS.from_epsg(self.epsg_code)  # type: ignore[arg-type]

        if self.crs_source == "wkt":
            return CRS.from_wkt(self.wkt_str)  # type: ignore[arg-type]

        if self.crs_source == "projjson":
            return CRS.from_json(self.projjson_str)  # type: ignore[arg-type]

        # 2) Custom CRS (без PROJ4!)
        if self.crs_source != "custom":
            raise ValueError("Неизвестный crs_source")

        # --- custom: latlon ---
        if self.ccrs_type == "latlon":
            # Здесь без PROJ4 проще и надёжнее через EPSG для известных датумов
            if self.datum == "WGS84":
                return CRS.from_epsg(4326)  # WGS 84 geographic
            if self.datum == "CGCS2000":
                return CRS.from_epsg(4490)  # CGCS2000 geographic
            if self.datum == "SK42":
                return CRS.from_epsg(4284)  # Pulkovo 1942 geographic (близко к SK42)
            raise ValueError(f"custom latlon datum={self.datum} пока не поддержан без EPSG/WKT/PROJJSON")

        # --- custom: projection ---
        if self.ccrs_type != "projection":
            raise ValueError("custom CRS: неизвестный ccrs_type")

        # UTM: самый чистый вариант — EPSG коды, без каких-либо строк
        if self.zone_family == "UTM":
            if self.datum != "WGS84":
                raise ValueError("UTM без PROJ4 сейчас поддерживаем только для datum='WGS84' (через EPSG:326/327).")

            if self.utm_zone is None or self.utm_hemisphere is None:
                raise ValueError("UTM требует utm_zone и utm_hemisphere")

            if not (1 <= self.utm_zone <= 60):
                raise ValueError("utm_zone должен быть 1..60")

            epsg = (32600 + self.utm_zone) if self.utm_hemisphere == "N" else (32700 + self.utm_zone)
            return CRS.from_epsg(epsg)

        # МСК (Красовский/SK42): делаем ProjectedCRS через PROJJSON,
        # базовую CRS берём по EPSG (Pulkovo 1942), чтобы не описывать датум вручную.
        if self.zone_family == "МСК":
            if self.datum != "SK42":
                raise ValueError("МСК (Красовский) требует datum='SK42'")

            # Эти параметры у вас уже валидируются
            if self.lon_0 is None or self.x_0 is None or self.y_0 is None:
                raise ValueError("МСК требует lon_0, x_0, y_0")

            base = CRS.from_epsg(4284)  # Pulkovo 1942 geographic CRS

            projjson = {
                "type": "ProjectedCRS",
                "name": "MSK (custom, SK42/Krassovsky)",
                "base_crs": json.loads(base.to_json()),  # встроим базовую CRS как PROJJSON
                "conversion": {
                    "type": "Conversion",
                    "name": "Transverse Mercator",
                    "method": {
                        "name": "Transverse Mercator",
                        "id": {"authority": "EPSG", "code": 9807},
                    },
                    "parameters": [
                        {
                            "name": "Latitude of natural origin",
                            "value": float(self.lat_0 or 0.0),
                            "unit": {"type": "AngularUnit", "name": "degree", "conversion_factor": 0.0174532925199433},
                            "id": {"authority": "EPSG", "code": 8801},
                        },
                        {
                            "name": "Longitude of natural origin",
                            "value": float(self.lon_0),
                            "unit": {"type": "AngularUnit", "name": "degree", "conversion_factor": 0.0174532925199433},
                            "id": {"authority": "EPSG", "code": 8802},
                        },
                        {
                            "name": "Scale factor at natural origin",
                            "value": float(self.k0 or 1.0),
                            "unit": {"type": "ScaleUnit", "name": "unity", "conversion_factor": 1.0},
                            "id": {"authority": "EPSG", "code": 8805},
                        },
                        {
                            "name": "False easting",
                            "value": float(self.x_0),
                            "unit": {"type": "LinearUnit", "name": "metre", "conversion_factor": 1.0},
                            "id": {"authority": "EPSG", "code": 8806},
                        },
                        {
                            "name": "False northing",
                            "value": float(self.y_0),
                            "unit": {"type": "LinearUnit", "name": "metre", "conversion_factor": 1.0},
                            "id": {"authority": "EPSG", "code": 8807},
                        },
                    ],
                },
                "coordinate_system": {
                    "type": "CartesianCS",
                    "subtype": "plane",
                    "axis": [
                        {"name": "Easting", "abbreviation": "E", "direction": "east", "unit": {"type": "LinearUnit", "name": "metre", "conversion_factor": 1.0}},
                        {"name": "Northing", "abbreviation": "N", "direction": "north", "unit": {"type": "LinearUnit", "name": "metre", "conversion_factor": 1.0}},
                    ],
                },
            }

            projected = projjson

            # Если это ГОСТ-вариант — добавляем явную связь с WGS84 через towgs84
            if self.msk_variant == "gost":
                if not self.towgs84:
                    raise ValueError("msk_variant='gost' требует towgs84")
                parts = [float(x.strip()) for x in self.towgs84.split(",")]
                if len(parts) != 7:
                    raise ValueError("towgs84 должен содержать 7 чисел: dx,dy,dz,rx,ry,rz,ds")

                dx, dy, dz, rx, ry, rz, ds = parts

                # Важно: в PROJJSON rotation обычно в arc-seconds, scale в ppm (как в towgs84 на сайте)
                bound = {
                    "type": "BoundCRS",
                    "source_crs": projected,
                    "target_crs": json.loads(CRS.from_epsg(4326).to_json()),
                    "transformation": {
                        "type": "Transformation",
                        "name": "towgs84 (7-parameter Helmert)",
                        "method": {"name": "Position Vector transformation (geocentric domain)",
                                   "id": {"authority": "EPSG", "code": 1033}},
                        "parameters": [
                            {"name": "X-axis translation", "value": dx,
                             "unit": {"type": "LinearUnit", "name": "metre", "conversion_factor": 1.0}},
                            {"name": "Y-axis translation", "value": dy,
                             "unit": {"type": "LinearUnit", "name": "metre", "conversion_factor": 1.0}},
                            {"name": "Z-axis translation", "value": dz,
                             "unit": {"type": "LinearUnit", "name": "metre", "conversion_factor": 1.0}},
                            {"name": "X-axis rotation", "value": rx,
                             "unit": {"type": "AngularUnit", "name": "arc-second",
                                      "conversion_factor": 4.84813681109536e-06}},
                            {"name": "Y-axis rotation", "value": ry,
                             "unit": {"type": "AngularUnit", "name": "arc-second",
                                      "conversion_factor": 4.84813681109536e-06}},
                            {"name": "Z-axis rotation", "value": rz,
                             "unit": {"type": "AngularUnit", "name": "arc-second",
                                      "conversion_factor": 4.84813681109536e-06}},
                            {"name": "Scale difference", "value": ds,
                             "unit": {"type": "ScaleUnit", "name": "parts per million", "conversion_factor": 1e-06}},
                        ],
                    },
                }

                return CRS.from_json(json.dumps(bound))

            # Иначе (calc) — как раньше
            return CRS.from_json(json.dumps(projected))

        if self.zone_family == "GK":
            raise ValueError("GK без PROJ4 пока не реализован: нужно зафиксировать правила параметров проекции (lon_0/x_0/y_0 и т.д.).")

        raise ValueError("Не удалось собрать CRS: некорректный набор параметров")

# 2. Воркфлоу