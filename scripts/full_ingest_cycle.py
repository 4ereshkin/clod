#!/usr/bin/env python3
r"""
Полный цикл инжеста: от создания компании до обработки скана через Temporal workflow.

Использование:
    python scripts/full_ingest_cycle.py \
        --company company1 \
        --dataset dataset1 \
        --cloud "data/user_data/НПС Крутое/1/t100pro_2025-04-28-08-36-08_CGCS.laz" \
        [--path "data/user_data/НПС Крутое/1/path.txt"] \
        [--cp "data/user_data/НПС Крутое/1/ControlPoint.txt"]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH для импорта модулей
script_dir = Path(__file__).parent
project_root = script_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from lidar_app.app.repo import Repo
from temporalio.client import Client
from temporalio.service import RPCError

from point_cloud.workflows.ingest_workflow import IngestWorkflowParams


def setup_company_crs_dataset(
    company_id: str,
    dataset_name: str,
    crs_id: str = "CGCS2000",
    crs_name: str = "CGCS2000",
) -> str:
    repo = Repo()

    print(f"1. Создание компании: {company_id}")
    repo.ensure_company(company_id, company_id)
    print(f"   ✓ Компания создана")

    print(f"2. Создание CRS: {crs_id}")
    repo.ensure_crs(
        crs_id=crs_id,
        name=crs_name,
        zone_degree=0,
        epsg=4490,
        units="m",
        axis_order="x_east,y_north,z_up",
    )
    print(f"   ✓ CRS создан")

    print(f"3. Ensure dataset by name: {dataset_name}")
    dataset_id = repo.ensure_dataset(
        company_id=company_id,
        crs_id=crs_id,
        name=dataset_name,
    )
    print(f"   ✓ Dataset ensured, id: {dataset_id}")
    return dataset_id


async def run_ingest_workflow(
    company_id: str,
    dataset_name: str,
    cloud_file: str,
    path_file: str | None = None,
    cp_file: str | None = None,
    crs_id: str = "CGCS2000",
    schema_version: str = "1.1.0",
    bump_version: bool = False,
    force: bool = False,
) -> None:
    """Запуск ingest workflow через Temporal."""
    client = await Client.connect("localhost:7233")

    # Подготовка артефактов
    artifacts = [
        {
            "kind": "raw.point_cloud",
            "local_file_path": str(Path(cloud_file).absolute()),
        }
    ]
    if path_file:
        artifacts.append({
            "kind": "raw.trajectory",
            "local_file_path": str(Path(path_file).absolute()),
        })
    if cp_file:
        artifacts.append({
            "kind": "raw.control_point",
            "local_file_path": str(Path(cp_file).absolute()),
        })

    params = IngestWorkflowParams(
        company_id=company_id,
        dataset_name=dataset_name,
        crs_id=crs_id,
        schema_version=schema_version,
        force=force,
        artifacts=artifacts,
        bump_version=bump_version
    )

    import time
    workflow_id = f"ingest-{company_id}-{dataset_name}-{int(time.time())}"

    print(f"4. Запуск ingest workflow через Temporal...")
    print(f"   Workflow ID: {workflow_id}")
    print(f"   Артефакты: {len(artifacts)}")

    from point_cloud.workflows.ingest_workflow import VERSION
    workflow_name = f"{VERSION}-ingest"

    handle = await client.start_workflow(
        workflow_name,
        params,
        id=workflow_id,
        task_queue="point-cloud-task-queue",
    )

    print(f"   ✓ Workflow запущен")

    try:
        result = await handle.result()
        print("\n=== Результат инжеста ===")
        print(f"Scan ID: {result.get('scan_id')}")
        print(f"Ingest Run ID: {result.get('ingest_run_id')}")
        print(f"Upload results: {len(result.get('upload_results', []))} артефактов")
        print(f"Errors: {len(result.get('errors', {}))}")
        if result.get('errors'):
            print("Ошибки:")
            for key, value in result['errors'].items():
                print(f"  - {key}: {value}")
        print("=== Успешно завершено ===")

    except RPCError as exc:
        print(f"⚠ Не удалось получить результат из Temporal: {exc}")
        print(f"Workflow продолжает выполняться. Проверьте статус в Temporal UI по ID {handle.id}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Полный цикл инжеста: от создания компании до обработки скана."
    )
    parser.add_argument(
        "--company",
        required=True,
        help="Company ID",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset name",
    )
    parser.add_argument(
        "--cloud",
        required=True,
        help="Path to point cloud file (.laz/.las)",
    )
    parser.add_argument(
        "--path",
        help="Path to trajectory file (optional)",
    )
    parser.add_argument(
        "--cp",
        help="Path to control point file (optional)",
    )
    parser.add_argument(
        "--crs",
        default="CGCS2000",
        help="CRS ID (default: CGCS2000)",
    )
    parser.add_argument(
        "--schema-version",
        default="1.1.0",
        help="Schema version (default: 1.1.0)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force creation even if ingest run already exists",
    )

    parser.add_argument(
        "--bump-version",
        action="store_true",
        help="Create a new dataset version before ingest",
    )

    args = parser.parse_args()

    # Проверка существования файлов
    if not Path(args.cloud).exists():
        print(f"Ошибка: файл {args.cloud} не найден")
        return

    if args.path and not Path(args.path).exists():
        print(f"Ошибка: файл {args.path} не найден")
        return

    if args.cp and not Path(args.cp).exists():
        print(f"Ошибка: файл {args.cp} не найден")
        return

    print("=== Полный цикл инжеста ===")
    print(f"Company: {args.company}")
    print(f"Dataset: {args.dataset}")
    print(f"Cloud: {args.cloud}")
    if args.path:
        print(f"Path: {args.path}")
    if args.cp:
        print(f"Control Point: {args.cp}")
    print()

    # Настройка БД (company, CRS, dataset)
    setup_company_crs_dataset(
        company_id=args.company,
        dataset_name=args.dataset,
        crs_id=args.crs,
    )

    print()

    # Запуск ingest workflow
    asyncio.run(
        run_ingest_workflow(
            company_id=args.company,
            dataset_name = args.dataset,
            cloud_file=args.cloud,
            path_file=args.path,
            cp_file=args.cp,
            crs_id=args.crs,
            bump_version=args.bump_version,
            schema_version=args.schema_version,
            force=args.force,
        )
    )


if __name__ == "__main__":
    main()

