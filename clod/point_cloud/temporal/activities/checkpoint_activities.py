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
from typing import Dict, List, Any
from temporalio import activity
from temporalio.exceptions import ApplicationError

from clod.checkpoint import Checkpoint, SelectFiles


@activity.defn
async def las_choice() -> List[str]:
    """
    Выбор файлов для пайплайна
    :return:
    """
    def _get_files() -> List[str]:
        file_paths = SelectFiles(template='templates/las.json').select_file()
        if file_paths is None:
            raise ApplicationError(
                f'Нужен как минимум 1 файл для работы пайплайна',
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
        cp = Checkpoint(file_path)

        if not cp.json_path or not cp.metadata:
            raise ApplicationError(
                f'Не удалось извлечь/сохранить метаданные для файла: {file_path}',
                non_retryable=True
            )
        return {
            'file_path': file_path,
            'metadata_json_path': cp.json_path,
            'metadata': cp.metadata,
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