"""
Activities related to metadata extraction and checkpointing.

These activities wrap the :class:`Checkpoint` class from the legacy
``point_cloud.checkpoint`` module.  Instead of prompting the user for
file selection via a GUI, the file paths are passed explicitly when the
activity is invoked.  The resulting metadata is written to the
``checkpoint_metadata`` directory as before and returned to the
workflow for subsequent processing.
"""

from __future__ import annotations

import asyncio
import json
import re

from typing import Dict, List, Any, Optional
from tkinter import Tk, filedialog
from pathlib import Path
from pdal import Reader
from pprint import pprint

from temporalio import activity
from temporalio.exceptions import ApplicationError
from .utilities import SelectOptions, Vec3
from dataclasses import dataclass


@dataclass(frozen=True)
class ControlPoint:
    lidar_xyz: Vec3
    world_xyz: Vec3
    gps_raw: Vec3
    meta: Dict[str, Any]



@dataclass(frozen=True)
class PathSample:
    idx: int
    t: float
    x: float
    y: float
    z: float
    imu1: float
    imu2: float
    imu3: float


@dataclass(frozen=True)
class ScanDescriptor:
    scan_id: str
    las_path: Optional[str]
    control_point_path: str
    path_txt_path: str

    control_point: ControlPoint
    path_head: List[PathSample]
    path_tail: List[PathSample]

    anchor_world_xyz: Vec3
    t_start: Optional[float]
    t_end: Optional[float]
    path_start_xyz: Optional[Vec3]
    path_end_xyz: Optional[Vec3]


@dataclass(frozen=True)
class RegistrationEdge:
    src_scan_id: str
    dst_scan_id: str
    kind: str
    reason: str
    approx_distance_m: float


@dataclass(frozen=True)
class OrderingResult:
    ordered_scan_ids: List[str]
    scans: Dict[str, ScanDescriptor]
    edges: List[RegistrationEdge]

@activity.defn
async def control_point_choice() -> List[str]:
    """
    Выбор файлов контрольных точек
    """
    def _get_files() -> List[str]:
        file_paths = SelectFiles(template='point_cloud/templates/points.json').select_file()
        if file_paths is None:
            raise ApplicationError(
                'Файлы не выбраны',
                non_retryable=True)
        return file_paths

    try:
        return await asyncio.to_thread(_get_files)
    except ApplicationError:
        raise
    except Exception as e:
        raise ApplicationError(f'Не удалось выбрать контрольные точки: {e}',
                               non_retryable=True) from e

@activity.defn
async def acq_path_choice() -> List[str]:
    """
    Выбор траекторий путей
    """
    def _get_files() -> List[str]:
        file_paths = SelectFiles(template='point_cloud/templates/acq_path.json').select_file()
        if file_paths is None:
            raise ApplicationError(
                'Файлы не выбраны',
                non_retryable=True)
        return file_paths

    try:
        return await asyncio.to_thread(_get_files)
    except ApplicationError:
        raise
    except Exception as e:
        raise ApplicationError(f'Не удалось выбрать маршруты съёмок: {e}',
                               non_retryable=True) from e

@activity.defn
async def parse_control_point_file(control_point_path: str) -> Dict[str, Any]:
    """
    Чтение ControlPoint.txt файла.

    Пример:
      the  x, y and z is 497043.405 6572625.565 114.151
      the gps x, y and z is 5916.13429930 6256.88924170 112.15130000
      degree: 6
      ellipsoid: cgcs2000
    """
    def _parse() -> ControlPoint:
        p = Path(control_point_path)
        if not p.exists():
            raise ApplicationError(f'ControlPoint не найден: {control_point_path}', non_retryable=True)

        text = p.read_text(encoding='utf-8', errors='ignore')

        m_world = re.search()


@activity.defn
async def las_choice() -> List[str]:
    """
    Выбор файлов для пайплайна
    :return:
    """
    def _get_files() -> List[str]:
        file_paths = SelectFiles(template='point_cloud/templates/las.json').select_file()
        if file_paths is None:
            raise ApplicationError(
                'Нужен как минимум 1 файл для работы пайплайна',
                        non_retryable=True)
        return file_paths

    try:
        return await asyncio.to_thread(_get_files)
    except ApplicationError:
        raise
    except Exception as e:
        raise ApplicationError(f'Не удалось выбрать LAS/LAZ файл: {e}',
                               non_retryable=True) from e


@activity.defn
async def load_metadata_for_file(file_path: str) -> Dict[str, Any]:

    def _extract() -> Dict[str, Any]:
        gw = Checkpoint(file_path)

        if not gw.json_path or not gw.metadata:
            raise ApplicationError(
                f'Не удалось извлечь/сохранить метаданные для файла: {file_path}',
                non_retryable=True
            )
        return {
            'file_path': file_path,
            'metadata_json_path': gw.json_path,
            'metadata': gw.metadata,
        }

    try:
        activity.heartbeat({'file_path': file_path, 'stage': 'start'})

        result = await asyncio.to_thread(_extract)

        activity.heartbeat({'file_path': file_path, 'stage': 'done'})
        return result

    except ApplicationError:
        raise
    except Exception as e:
        raise ApplicationError(
            f'Неожиданная ошибка при обработке {file_path}: {e}',
            non_retryable=False,
        ) from e


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

            reader = Reader.las(filename=path)
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