#!/bin/bash
# Полный цикл инжеста: от создания компании до обработки скана
# 
# Использование:
#   ./scripts/full_ingest_cycle.sh <company_id> <dataset_id> <cloud_file> [path_file] [cp_file]
#
# Пример:
#   ./scripts/full_ingest_cycle.sh company1 dataset1 data/user_data/НПС\ Крутое/1/t100pro_2025-04-28-08-36-08_CGCS.laz

set -e

COMPANY_ID=${1:-"test-company"}
DATASET_ID=${2:-"test-dataset"}
CLOUD_FILE=${3:-""}
PATH_FILE=${4:-""}
CP_FILE=${5:-""}

if [ -z "$CLOUD_FILE" ]; then
    echo "Использование: $0 <company_id> <dataset_id> <cloud_file> [path_file] [cp_file]"
    echo "Пример: $0 company1 dataset1 data/user_data/НПС\ Крутое/1/t100pro_2025-04-28-08-36-08_CGCS.laz"
    exit 1
fi

echo "=== Полный цикл инжеста ==="
echo "Company: $COMPANY_ID"
echo "Dataset: $DATASET_ID"
echo "Cloud file: $CLOUD_FILE"
echo ""

# 1. Создание компании
echo "1. Создание компании..."
python -m lidar_app.app.cli ensure-company --company "$COMPANY_ID" --name "$COMPANY_ID"

# 2. Создание CRS (используем CGCS2000 как пример)
echo "2. Создание CRS..."
python -m lidar_app.app.cli ensure-crs \
    --crs "CGCS2000" \
    --name "CGCS2000" \
    --zone-degree 0 \
    --epsg 4490 \
    --units "m" \
    --axis-order "x_east,y_north,z_up"

# 3. Создание датасета
echo "3. Создание датасета..."
python -m lidar_app.app.cli create-dataset \
    --company "$COMPANY_ID" \
    --dataset "$DATASET_ID" \
    --crs "CGCS2000" \
    --name "$DATASET_ID"

# 4. Запуск ingest workflow через Temporal
echo "4. Запуск ingest workflow через Temporal..."

ARTIFACT_ARGS="--artifact raw.point_cloud:$CLOUD_FILE"
if [ -n "$PATH_FILE" ]; then
    ARTIFACT_ARGS="$ARTIFACT_ARGS --artifact raw.trajectory:$PATH_FILE"
fi
if [ -n "$CP_FILE" ]; then
    ARTIFACT_ARGS="$ARTIFACT_ARGS --artifact raw.control_point:$CP_FILE"
fi

python -m clod.point_cloud.temporal.client.start_ingest \
    --company "$COMPANY_ID" \
    --dataset "$DATASET_ID" \
    --dataset-name "$DATASET_ID" \
    --crs "CGCS2000" \
    $ARTIFACT_ARGS

echo ""
echo "=== Цикл завершён ==="

