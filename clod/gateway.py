from tkinter import Tk, filedialog
from typing import List, Tuple
from dataclasses import dataclass

import pdal
import json
import pprint
from pathlib import Path

# TODO: Надо как-то поднять выше чекпоинта
class SelectOptions:
    template: str
    title: str = ''
    filter_name: str = ''
    extensions_str: str = ''

    def read_options(self):
        with open(self.template, 'r', encoding='utf-8') as f:
            options = json.load(f)
        self.title = options['title']
        self.filter_name = options['file_types']['filter_name']
        self.extensions_str = ' '.join(options['file_types']['types'])

    def filetypes(self) -> List[Tuple[str, str]]:
        return [(self.filter_name, self.extensions_str)]


class SelectFiles:
    def __init__(self, template: str):
        self.options = SelectOptions()
        self.options.template = template
        self.options.read_options()

    def select_file(self):
        root = Tk()
        root.withdraw()

        files = filedialog.askopenfilenames(
            title=self.options.title,
            filetypes=self.options.filetypes()
        )

        root.destroy()

        if files:
            return list(files)
        return None

class Checkpoint:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.metadata: dict = {}
        self.json_path: str | None = None

        meta = self._load_metadata(path=self.file_path)

        if meta is False:
            self.metadata = {}
            self.json_path = None
            return

        self.metadata = meta

        jp = self._save_metadata_to_json(file_path=self.file_path, metadata=self.metadata)
        if not jp:
            self.json_path = None
            return
        self.json_path = jp

    def _load_metadata(self, path: str) -> dict | bool:
        try:
            if not isinstance(path, str) or not path.strip():
                print('Пустой путь к файлу')
                return False

            p = Path(path)
            if not p.exists():
                print(f'Файл {path} не найден')

            reader = pdal.Reader.las(filename=path)
            pipe = reader.pipeline()
            pipe.execute()

            meta = pipe.metadata

            if not meta:
                print(f'Ошибка извлечения метаданных: информация о метаданных отсутствует для {path}')
                return False

            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except json.JSONDecodeError as e:
                    print(f'Ошибка парсинга JSON метаданных: {e}')
                    return False

            if not isinstance(meta, dict):
                print(f"Ошибка: неожиданная структура метаданных ({type(meta)}): {path}")
                return False

            payload = meta.get('metadata', meta)
            print(f"Метаданные успешно загружены для: {p.name}")

            return payload
        except Exception as e:
            print(f'Ошибка извлечения метаданных PDAL: {e}')
            return False


    def _save_metadata_to_json(self, file_path: str, metadata: dict) -> str | bool:
        try:
            if not isinstance(file_path, str) or not file_path.strip():
                print('Пустой путь при сохранении JSON')
                return False

            if not isinstance(metadata, dict):
                print('Метаданные должны быть dict')
                return False

            output_dir = Path("data/checkpoint_metadata")
            output_dir.mkdir(parents=True, exist_ok=True)

            file_name = Path(self.file_path).stem
            json_filename = output_dir / f"metadata_{file_name}.json"

            print(f'Сохраняем метаданные в: {json_filename}')
            pprint.pprint(metadata, indent=2)

            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)

            if json_filename.exists():
                file_size = json_filename.stat().st_size
                print(f'Файл создан, размер: {file_size} байт')
                return str(json_filename)
            else:
                print(f'Ошибка: файл {json_filename} не создан')
                return False
        except Exception as e:
            print(f'Ошибка сохранения метаданных в JSON: {e}')
            return False