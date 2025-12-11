from tkinter import Tk, filedialog
import pdal
import json
import pprint
from pathlib import Path


class Checkpoint():
    
    def __init__(self):
        self.file_path = []
        
        self.tk_root = Tk()
        self.tk_root.withdraw()

        self.metadata_json_path = {}
        self.cloud_metadata = {}

    def run(self):
        try:
            if not self.select_file():
                print('Ошибка выбора файла(ов)')
                return False

            for file in self.file_path:
                if not self.load_metadata(path=file):
                    print(f'Не удалось загрузить метаданные для файла: {file}')
                    return False

            if self.metadata_to_json():
                return True
            
            print('Ошибка чекпоинта: не удалось сохранить метаданные в JSON файл')
            return False

        except Exception as e:
            print(f'Ошибка чекпоинта: {e}')
        return False

    def select_file(self):

        root = self.tk_root

        files_path = filedialog.askopenfilenames(
            title="Выберите .LAS или .LAZ файл",
            filetypes=[("LAS/LAZ файлы", "*.las *.laz")])
        root.destroy()

        if files_path:
            
            self.file_path = list(files_path)
            return True
        else:
            return False
        
    def load_metadata(self, path: str) -> bool:

        check_file = str(path)
        try:
            reader = pdal.Reader.las(filename=check_file)
            pipe = reader.pipeline()
            pipe.execute()
        except Exception as e:
            print(f'Ошибка извлечения метаданных: {e}')
            return False

        meta = pipe.metadata

        if meta:
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except json.JSONDecodeError as e:
                    print(f'Ошибка парсинга JSON метаданных: {e}')
                    return False
            
            if 'metadata' in meta:
                self.cloud_metadata[path] = meta['metadata']
                print(f'Метаданные успешно загружены для: {Path(path).name}')
                return True
            else:
                print(f'Структура метаданных не содержит ключ "metadata" для: {path}')
                print(f'Доступные ключи: {list(meta.keys())}')
                # Сохраняем все метаданные, если структура отличается
                self.cloud_metadata[path] = meta
                return True


        else:
            print(f'Ошибка извлечения метаданных: информация о метаданных отсутствует для {path}')
            return False

    def metadata_to_json(self) -> bool:
        try:
            output_dir = Path("activities_data/checkpoint_metadata")
            output_dir.mkdir(exist_ok=True)
            
            for file_path, metadata in self.cloud_metadata.items():
                file_name = Path(file_path).stem
                json_filename = f"activities_data/checkpoint_metadata/metadata_{file_name}.json"
                self.metadata_json_path[file_name] = json_filename
                
                print(f'Сохраняем метаданные в: {json_filename}')
                pprint.pprint(f'Данные для сохранения: {metadata}', indent=2)
                
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
                
                # Проверяем что файл создан и не пустой
                if Path(json_filename).exists():
                    file_size = Path(json_filename).stat().st_size
                    print(f'Файл создан, размер: {file_size} байт')
                else:
                    print(f'Ошибка: файл {json_filename} не создан')
                    
            return True
        except Exception as e:
            print(f'Ошибка сохранения метаданных в JSON: {e}')