from __future__ import annotations

import glob
import os
from pathlib import Path

from pyproj import CRS, Transformer


INPUT_FOLDER = "input_data"
OUTPUT_FOLDER = "output_data"
COL_X = 2
COL_Y = 3

PROJ_WKT = """
PROJCS["CGCS2000 / Gauss-Kruger zone 11",
    GEOGCS["CGCS 2000",
        DATUM["China_2000",
            SPHEROID["CGCS2000",6378137,298.257222101]],
        PRIMEM["Greenwich",0],
        UNIT["degree",0.0174532925199433]],
    PROJECTION["Transverse_Mercator"],
    PARAMETER["latitude_of_origin",0],
    PARAMETER["central_meridian",63],
    PARAMETER["scale_factor",1],
    PARAMETER["false_easting",500000],
    PARAMETER["false_northing",0],
    UNIT["metre",1]]
"""


def is_data_line(line: str) -> bool:
    parts = line.strip().split()
    return bool(parts) and parts[0].isdigit() and len(parts) > 4


def process_files(input_folder: str = INPUT_FOLDER, output_folder: str = OUTPUT_FOLDER) -> None:
    try:
        source_crs = CRS.from_wkt(PROJ_WKT)
        target_crs = CRS.from_epsg(4326)
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=False)
    except Exception as e:
        raise RuntimeError(f"Projection initialization failed: {e}") from e

    os.makedirs(output_folder, exist_ok=True)

    files = glob.glob(os.path.join(input_folder, "*.txt"))
    if not files:
        return

    for file_path in files:
        file_name = os.path.basename(file_path)
        output_path = os.path.join(output_folder, f"geo_{file_name}")

        with open(file_path, 'r', encoding='utf-8') as fin, open(output_path, 'w', encoding='utf-8') as fout:
            for line in fin:
                if not is_data_line(line):
                    fout.write(line)
                    continue

                parts = line.strip().split()
                try:
                    x = float(parts[COL_X])
                    y = float(parts[COL_Y])
                    lat, lon = transformer.transform(x, y)
                    fout.write(f"{line.strip()} {lat:.8f} {lon:.8f}\n")
                except (ValueError, IndexError):
                    fout.write(line)


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    default_input = root / "samples" / "input_data"
    process_files(str(default_input), str(root / "samples" / "output_data"))
