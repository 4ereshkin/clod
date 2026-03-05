import os
import json
from typing import Optional

import pdal
from pathlib import Path

from temporalio import activity
from temporalio.exceptions import ApplicationError

from infrastructure.s3 import S3Client
from infrastructure.infrastructure_config import S3Config


@activity.defn
async def download_s3_object(key: str, dst_dir: str) -> str:
    """
    Асинхронно скачивает объект из S3 по ключу.
    Возвращает локальный путь к скачанному файлу.
    """
    activity.heartbeat({"stage": "downloading", "key": key})

    # Инициализируем клиент с конфигом из переменных окружения
    s3_config = S3Config(**os.environ)
    s3_client = S3Client(s3_config)

    # Формируем путь для сохранения
    filename = Path(key).name
    dst_path = Path(dst_dir)
    dst_path.mkdir(parents=True, exist_ok=True)

    local_file_path = dst_path / filename

    # Вызываем готовый метод инфраструктуры
    await s3_client.download_object(key=key, dest_path=str(local_file_path))

    return str(local_file_path)


@activity.defn
async def upload_s3_object(local_path: str, key: str) -> dict[str, str]:
    """
    Загружает файл в S3 и возвращает {'s3_key': key, 'etag': etag}.
    Для etag используем md5 локального файла, так как MultipartUpload
    формирует сложный etag, но md5 подойдет как хэш файла.
    """
    s3_config = S3Config(**os.environ)
    s3_client = S3Client(s3_config)

    # Считаем MD5 перед загрузкой, чтобы вернуть как etag
    etag, _ = s3_client._calc_md5(local_path)

    # Загрузка
    await s3_client.upload_object(file_path=local_path, object_name=key)

    return {"s3_key": key, "etag": etag}


@activity.defn
def compute_point_cloud_stats(cloud_path: str, dst_json: str) -> str:
    """
    Вычисляет статистику облака точек через PDAL (filters.stats)
    и сохраняет результат в JSON-файл dst_json. Возвращает путь к файлу.
    """
    activity.heartbeat({"stage": "computing_stats"})

    # Используем pdal Python API: только чтение и фильтр stats
    pipeline_json = [
        {
            "type": "readers.las",
            "filename": cloud_path
        },
        {
            "type": "filters.stats"
        }
    ]

    try:
        pipeline_spec = json.dumps(pipeline_json)
        pipeline = pdal.Pipeline(pipeline_spec)
        pipeline.execute()
    except Exception as exc:
        raise ApplicationError(f"Failed to execute PDAL stats pipeline: \n{exc}")

    # Получаем метаданные, в которых лежит результат работы фильтра
    raw_metadata = pipeline.metadata['metadata']
    try:
        metadata = json.loads(raw_metadata) if isinstance(raw_metadata, str) else raw_metadata
    except Exception as exc:
        raise ApplicationError(f"Failed to decode PDAL metadata: \n{exc}")

    # Извлекаем статистику
    stats_data = metadata.get("filters.stats", {})

    # Сохраняем в файл
    with open(dst_json, 'w', encoding='utf-8') as f:
        json.dump(stats_data, f, indent=2)

    return dst_json


@activity.defn
def save_dict_to_json(data: dict, dst_path: str) -> str:
    """Сохраняет словарь в JSON файл."""
    with open(dst_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    return dst_path


@activity.defn
def reproject_to_copc(file_path: str, in_srs: str, out_srs: str) -> Optional[str]:
    activity.heartbeat({'stage': 'reproject_to_copc'})

    local_in = Path(file_path)
    local_out = local_in.with_name(f'{local_in.stem}_copc.laz')

    pipeline = {
        "pipeline": [
            {"type": "readers.las", "filename": str(local_in)},
            {"type": "filters.reprojection", "in_srs": in_srs, "out_srs": out_srs},
            {"type": "writers.copc", "filename": str(local_out)}
        ]
    }

    try:
        pipe = pdal.Pipeline(json.dumps(pipeline))
        pipe.execute()
    except Exception as exc:
        raise RuntimeError(f"Failed to execute PDAL reprojection pipeline: \n{exc}") from exc

    if not local_out.exists():
        raise RuntimeError(f'PDAL reprojection pipeline produced no output: {local_out}')

    return str(local_out)