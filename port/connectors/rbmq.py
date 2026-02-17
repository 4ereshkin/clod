import os
import glob
import re
from pyproj import CRS, Transformer

# ================= НАСТРОЙКИ =================

INPUT_FOLDER = "input_data"  # Папка с исходными txt
OUTPUT_FOLDER = "output_data"  # Папка для готовых файлов

# Настройка колонок (в ваших данных это 2-я и 3-я, считая с 0)
# Пример: 0 1745818569.119 [497043.355] [6572625.460] ...
COL_X = 2
COL_Y = 3

# ================= НАСТРОЙКИ ПРОЕКЦИИ (Зона 11) =================

# Создаем проекцию для 11-й зоны (CM=63)
# Параметры подобраны под ваши данные (CGCS2000 / GK Zone 11)
proj_wkt = """
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


# Обратите внимание: central_meridian = 63 (для ваших новых координат)

# ================= ЛОГИКА =================

def is_data_line(line):
    """Проверяет, является ли строка строкой с данными (начинается с цифры индекса)"""
    parts = line.strip().split()
    if not parts:
        return False
    # Ваша строка данных всегда начинается с целого числа-индекса (0, 1, 2...)
    # А заголовки содержат текст ("the", "degree", "ellipsoid" и т.д.)
    if parts[0].isdigit() and len(parts) > 4:
        return True
    return False


def process_files():
    # 1. Инициализация трансформации
    try:
        source_crs = CRS.from_wkt(proj_wkt)
        target_crs = CRS.from_epsg(4326)  # WGS84
        # always_xy=True -> (Lon, Lat)
        # always_xy=False -> (Lat, Lon) - выберем это, чтобы Latitude была первой
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=False)
        print(f"✅ Проекция настроена: Gauss-Kruger Zone 11 (CM 63E)")
    except Exception as e:
        print(f"❌ Ошибка настройки проекции: {e}")
        return

    # 2. Создаем выходную папку
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    # 3. Обработка файлов
    files = glob.glob(os.path.join(INPUT_FOLDER, "*.txt"))
    if not files:
        print(f"⚠️ Папка '{INPUT_FOLDER}' пуста или не существует.")
        return

    print(f"Найдено файлов: {len(files)}")

    for file_path in files:
        file_name = os.path.basename(file_path)
        print(f"🔄 Обработка: {file_name} ...", end="")

        output_path = os.path.join(OUTPUT_FOLDER, f"geo_{file_name}")

        count = 0
        with open(file_path, 'r', encoding='utf-8') as fin, \
                open(output_path, 'w', encoding='utf-8') as fout:

            for line in fin:
                line_stripped = line.strip()

                # Если это не строка с данными (заголовок), пишем как есть
                if not is_data_line(line):
                    fout.write(line)
                    continue

                # Обработка строки с данными
                parts = line_stripped.split()
                try:
                    x = float(parts[COL_X])
                    y = float(parts[COL_Y])

                    # Трансформация (Lat, Lon)
                    lat, lon = transformer.transform(x, y)

                    # Формируем новую стр   оку: добавляем Lat Lon в конец
                    # Используем .8f для высокой точности GPS
                    new_line = f"{line_stripped} {lat:.8f} {lon:.8f}\n"
                    fout.write(new_line)
                    count += 1
                except (ValueError, IndexError):
                    # Если сбой парсинга, пишем строку как была (или логируем ошибку)
                    fout.write(line)

        print(f" Готово! ({count} точек)")

    print(f"\n🎉 Все файлы обработаны. Результат в папке '{OUTPUT_FOLDER}'")


if __name__ == "__main__":
    if not os.path.exists(INPUT_FOLDER):
        os.makedirs(INPUT_FOLDER)
        print(f"Создана папка {INPUT_FOLDER}. Положите туда txt файлы.")
    else:
        process_files()
