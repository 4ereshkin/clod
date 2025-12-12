"""
Workflow for orchestration of point cloud processing.

This workflow coordinates a sequence of activities to extract metadata
from raw point cloud files, reproject them into a common spatial
reference system, insert the results into a database and optionally
generate 3D tiles.  The workflow is designed to be idempotent and
replayable, making use of Temporal's deterministic execution model and
retriable activities.

The workflow accepts a list of file paths and optional parameters for
coordinate system transformation and database configuration.  It
returns a summary of the operations performed, including the paths of
the reprojected files and whether tile generation succeeded for each.
"""

from __future__ import annotations

import json
import time
import asyncio
from datetime import timedelta
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from fontTools.misc.arrayTools import insetRect
from temporalio import workflow
from temporalio.common import RetryPolicy


@dataclass
class MlsPipelineParams:
    in_srs: str = "EPSG:4490"
    out_srs: str = "EPSG:4326"
    db_config_path: str = "db.json"
    generate_tiles: bool = False


@workflow.defn
class MlsPipelineWorkflow:
    def __init__(self) -> None:
        self._file_paths: Optional[List[str]] = None

        self._processed_files: int = 0
        self._stage: str = 'Ожидает файлы'
        self._errors: dict[str, str] = {}

    @workflow.signal
    async def las_selected(self, file_paths: List[str]) -> None:
        cleaned = [p for p in file_paths if isinstance(p, str) and p.strip()]
        self._file_paths = cleaned
        if cleaned:
            self._stage = 'Файлы получены'
        else:
            self._stage = 'Файлы не получены, ожидается выбор через GUI'

    @workflow.query
    def progress(self) -> dict:
        total = len(self._file_paths) if self._file_paths else 0
        return {
            'stage': self._stage,
            'total_files': total,
            'processed_files': self._processed_files,
            'errors': self._errors
        }

    @workflow.run
    async def run(self, params: MlsPipelineParams) -> Dict[str, Any]:

        # Разворачиваем параметры во внутренние переменные
        in_srs = params.in_srs
        out_srs = params.out_srs
        db_config_path = params.db_config_path
        generate_tiles = params.generate_tiles

        await workflow.wait_condition(lambda: self._file_paths is not None)
        file_paths = self._file_paths or []

        if not file_paths:
            self._stage = 'Выбор файлов через GUI'
            file_paths = await workflow.execute_activity(
                'las_choice',
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
            self._file_paths = file_paths
            self._stage = 'Файлы получены через GUI'

        self._stage = 'Извлечение метаданных'

        meta_futures = [
            workflow.execute_activity(
                'load_metadata_for_file',
                 args=[file_path],
                 schedule_to_close_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(maximum_attempts=3),
                 )
            for file_path in file_paths]

        meta_result = await asyncio.gather(*meta_futures, return_exceptions=True)
        meta_ok: dict[str, Any] = {}
        meta_fail: dict[str, str] = {}

        for path, res in zip(file_paths, meta_result):
            if isinstance(res, Exception):
                msg = f'Не удалось извлечь метаданные: {res}'
                self._errors[path] = msg
                meta_fail[path] = msg
            else:
                meta_ok[path] = res
            self._processed_files += 1

        file_paths = list(meta_ok.keys())

        # 2. Репроекция каждого файла (параллельно)
        self._stage = 'Репроекция'
        reproject_futures = [
            workflow.execute_activity("reproject_file",
                args=[file_path, in_srs, out_srs],
                heartbeat_timeout=timedelta(minutes=10),
                schedule_to_close_timeout=timedelta(hours=3),
                retry_policy=RetryPolicy(maximum_attempts=1)
                 )
            for file_path in file_paths]

        reproject_result = await asyncio.gather(*reproject_futures, return_exceptions=True)
        reproject_ok: List[str] = []
        reproject_fail: dict[str, str] = {}

        for path, res in zip(file_paths, reproject_result):
            if isinstance(res, Exception):
                msg = f"reproject step failed: {res}"
                self._errors[path] = msg
                reproject_fail[path] = msg
            else:
                # предполагаем, что activity возвращает путь к репроецированному файлу
                reproject_ok.append(res)
            self._processed_files += 1


        # 3. Вставка в БД (по одному, чтобы не валить коннект)
        self._stage = 'Запись в БД'
        insert_futures = [workflow.execute_activity(
                "insert_file_into_db",
                args=[file_path, db_config_path],
                schedule_to_close_timeout=timedelta(hours=3),
                retry_policy=RetryPolicy(maximum_attempts=2),
            ) for file_path in reproject_ok]

        insert_result = await asyncio.gather(*insert_futures, return_exceptions=True)

        insert_ok: List[Any] = []
        insert_failed: dict[str, str] = {}

        for path, res in zip(reproject_ok, insert_result):
            if isinstance(res, Exception):
                msg = f"db insert failed: {res}"
                self._errors[path] = msg
                insert_failed[path] = msg
            else:
                insert_ok.append(res)
            self._processed_files += 1


        # 4. Опционально делаем 3D Tiles
        tiles_results: List[bool] = []  # инициализируем всегда, чтобы не было UnboundLocalError
        if generate_tiles:
            self._stage = '3D Tiles генерация'
            tile_futures = []
            for file_path in reproject_ok:
                fut = workflow.execute_activity(
                    "convert_to_tileset",
                    args=[file_path, "cesium_tiles"],
                    schedule_to_close_timeout=timedelta(seconds=3600),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
                tile_futures.append(fut)
            tiles_results = await asyncio.gather(*tile_futures)

        self._stage = 'Завершение процесса'

        # 5. Возвращаем аккуратный JSON-совместимый результат
        return {
            "input_files": file_paths,
            "metadata_ok": meta_ok,
            "metadata_failed": meta_fail,
            "reprojected_files": reproject_ok,
            "reproject_failed": reproject_fail,
            "db_insert_ok": insert_ok,
            "db_insert_failed": insert_failed,
            # "tiles_ok": tiles_ok,
            # "tiles_failed": tiles_failed,
        }