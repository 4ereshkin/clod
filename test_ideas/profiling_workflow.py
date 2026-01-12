from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
import yaml

from download_workflow import DownloadWorkflowParams

with open(r'D:\1_prod\point_cloud\config.yaml', 'r') as f:
    VERSION = yaml.safe_load(f.read())['VERSION_INFO']['WORKFLOW_VERSION']


@dataclass
class ProfilingWorkflowParams:
    scan_id: str
    cloud_path: str
    geojson_dst: str


@workflow.defn(name=f'{VERSION}-profiling')
class ProfilingWorkflow:
    def __init__(self):
        self._stage = 'Initialize'

    @workflow.query
    async def progress(self):
        return {
            'stage': self._stage,
                }


    @workflow.run
    async def run(self, params: ProfilingWorkflowParams):
        self._stage = 'Downloading file'

        files_by_kind: Dict[str, str] = await workflow.execute_child_workflow(
            f"{VERSION}-download",
            DownloadWorkflowParams(
                scan_id=params.scan_id,
                dst_dir=params.cloud_path,
                kinds=["raw.point_cloud"],
            ),
        )

        cloud_file = files_by_kind["raw.point_cloud"]

        self._stage = 'Profiling files'

        meta = await workflow.execute_activity(
            "point_cloud_meta",
            args=[cloud_file, params.geojson_dst],
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

        self._stage = 'Reading hexbin GeoJSON'
        geojson = await workflow.execute_activity(
            "read_cloud_hexbin",
            args=[params.geojson_dst],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

        self._stage = 'Extracting hexbin GeoJSON fields'
        hexbin_fields = await workflow.execute_activity(
            "extract_hexbin_fields",
            args=[geojson],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

        self._stage = 'Uploading hexbin GeoJSON'
        upload_info = await workflow.execute_activity(
            "upload_hexbin",
            args=[params.scan_id, params.geojson_dst],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

        self._stage = 'Uploading profiling manifest'
        manifest_info = await workflow.execute_activity(
            "upload_profiling_manifest",
            args=[params.scan_id, meta, hexbin_fields, upload_info],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )



        self._stage = 'Aggregating results'


"""
1. Первичное профилирование (обязательно)

Цель — понять, с чем вы работаете, прежде чем что-то удалять.

Что делаем:

диапазоны X/Y/Z

плотность точек

количество точек

наличие атрибутов (RGB, intensity, classification)

масштаб и система координат

Почему важно:
в книге подчёркивается, что ошибки на этом шаге делают весь последующий pipeline некорректным.

2. Приведение системы координат и масштаба

Перед любыми алгоритмами данные должны быть:

в одной CRS (если LiDAR/геоданные)

с нормализованным масштабом, если дальше ML/PCA

Типично:

перевод в локальную систему

центрирование (mean subtraction)

опционально — scale normalization

3. Downsampling (сначала, а не потом)

В книге чётко:
сначала уменьшение плотности, потом фильтрация.

Основной метод:

voxel grid downsampling

Зачем:

стабилизирует статистику соседства

уменьшает вычислительную сложность

делает шумовые фильтры более устойчивыми

4. Удаление шума (Noise Removal)

Это не ручная очистка, а статистическая процедура.

Используются:

Statistical Outlier Removal (SOR)

Radius Outlier Removal (ROR)

Ключевая идея из книги:

шум — это точки с нарушенной локальной топологией, а не «плохие на глаз»

5. Фильтрация по интересующей области (ROI)

На этом этапе:

spatial cropping

height thresholding

class-based filtering (если есть semantic labels)

Важно:
ROI не равен сегментации, это лишь ограничение сцены.

6. Оценка нормалей (если дальше geometry / ML)

Обязательный шаг перед:

RANSAC

region growing

feature extraction

PCA / local descriptors

Нормали считаются:

после downsampling

после удаления выбросов

7. Проверка готовности к следующему этапу

В книге это выделено как отдельный логический контроль:

Проверьте:

однородность плотности

отсутствие «пустых» зон

корректность ориентации нормалей

воспроизводимость параметров

Если данные не проходят эту проверку, переходить к регистрации или ML нельзя.

Итоговый пайплайн (как в книге)
Load
 → Profiling
 → CRS / Scale normalization
 → Downsampling
 → Noise filtering
 → ROI filtering
 → Normal estimation
 → Ready for registration / segmentation / ML

Ключевая философия книги (важно)

Препроцессинг — это инженерный контракт, а не набор хаотичных фильтров

Любое удаление точек должно быть обосновано геометрически

Ошибки здесь мультиплицируются дальше (особенно в ML)

Если хочешь, в следующем шаге могу:

разобрать конкретный пример (LiDAR / фотограмметрия / indoor)

показать типовые параметры (voxel size, k, radius)

перевести этот pipeline в Open3D / PyVista код

Скажи, какой тип облака и под какую задачу.
"""
