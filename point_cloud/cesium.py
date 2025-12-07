from pathlib import Path
from py3dtiles import convert


class CesiumImport:
    def __init__(self, cloud_path: str = "", output_dir: str = "cesium_tiles",
                  in_srs: str = "EPSG:4326", out_srs: str = "EPSG:4978"):
        self.cloud_path = cloud_path
        self.output_dir = output_dir

    def _convert_to_tileset(self) -> bool:
        try:
            convert.convert(
                files = [str(self.cloud_path)],
                outfolder = str(self.output_dir)
            )
            return True
        except Exception as e:
            print(f'Ошибка конвертации в 3D Tiles: {e}')
            return False
    
    def run(self) -> bool:
        if not Path(self.cloud_path).exists():
            print(f'Файл не найден: {self.cloud_path}')
            return False
        if not self._convert_to_tileset():
            return False
        return True