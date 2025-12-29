# Скрипты для полного цикла инжеста

## Описание

Скрипты для автоматизации полного цикла инжеста облаков точек:
1. Создание компании (company)
2. Создание системы координат (CRS)
3. Создание датасета (dataset)
4. Запуск ingest workflow через Temporal
5. Автоматическая загрузка артефактов в S3 и обработка

## Предварительные требования

1. **Запущен Temporal сервер** (localhost:7233)
   ```bash
   docker-compose up temporal
   ```

2. **Запущен Temporal worker**
   ```bash
   python -m clod.point_cloud.temporal.workers.worker_orchestrator
   ```

3. **База данных PostgreSQL** настроена и доступна (по умолчанию localhost:5433)

4. **MinIO/S3** настроен и доступен (по умолчанию localhost:9000)

## Использование

### Python скрипт (рекомендуется)

```bash
python scripts/full_ingest_cycle.py \
    --company company1 \
    --dataset dataset1 \
    --cloud "data/user_data/НПС Крутое/1/t100pro_2025-04-28-08-36-08_CGCS.laz" \
    --path "data/user_data/НПС Крутое/1/path.txt" \
    --cp "data/user_data/НПС Крутое/1/ControlPoint.txt"
```

**Параметры:**
- `--company` (обязательно) - ID компании
- `--dataset` (обязательно) - ID датасета
- `--cloud` (обязательно) - путь к файлу облака точек (.laz/.las)
- `--path` (опционально) - путь к файлу траектории
- `--cp` (опционально) - путь к файлу контрольных точек
- `--crs` (опционально, по умолчанию "CGCS2000") - ID системы координат
- `--schema-version` (опционально, по умолчанию "1.1.0") - версия схемы
- `--force` (опционально) - принудительное создание, даже если ingest run уже существует

### Bash скрипт (Linux/Mac)

```bash
chmod +x scripts/full_ingest_cycle.sh
./scripts/full_ingest_cycle.sh \
    company1 \
    dataset1 \
    "data/user_data/НПС Крутое/1/t100pro_2025-04-28-08-36-08_CGCS.laz" \
    "data/user_data/НПС Крутое/1/path.txt" \
    "data/user_data/НПС Крутое/1/ControlPoint.txt"
```

## Пошаговый процесс вручную

Если вы хотите выполнить шаги вручную:

### 1. Создание компании

```bash
python -m lidar_app.app.cli ensure-company --company company1 --name "Company 1"
```

### 2. Создание CRS

```bash
python -m lidar_app.app.cli ensure-crs \
    --crs CGCS2000 \
    --name "CGCS2000" \
    --zone-degree 0 \
    --epsg 4490 \
    --units m \
    --axis-order "x_east,y_north,z_up"
```

### 3. Создание датасета

```bash
python -m lidar_app.app.cli create-dataset \
    --company company1 \
    --dataset dataset1 \
    --crs CGCS2000 \
    --name "Dataset 1"
```

### 4. Запуск ingest workflow

```bash
python -m clod.point_cloud.temporal.client.start_ingest \
    --company company1 \
    --dataset dataset1 \
    --dataset-name "Dataset 1" \
    --crs CGCS2000 \
    --artifact raw.point_cloud:"data/user_data/НПС Крутое/1/t100pro_2025-04-28-08-36-08_CGCS.laz" \
    --artifact raw.trajectory:"data/user_data/НПС Крутое/1/path.txt" \
    --artifact raw.control_point:"data/user_data/НПС Крутое/1/ControlPoint.txt"
```

## Пример полного цикла

```bash
# 1. Убедитесь, что Temporal сервер запущен
docker-compose up -d temporal

# 2. Запустите worker в отдельном терминале
python -m clod.point_cloud.temporal.workers.worker_orchestrator

# 3. В другом терминале запустите скрипт
python scripts/full_ingest_cycle.py \
    --company test-company \
    --dataset test-dataset \
    --cloud "data/user_data/НПС Крутое/1/t100pro_2025-04-28-08-36-08_CGCS.laz" \
    --path "data/user_data/НПС Крутое/1/path.txt" \
    --cp "data/user_data/НПС Крутое/1/ControlPoint.txt"
```

## Что происходит в workflow

1. **Ensure company** - проверка/создание компании в БД
2. **Ensure CRS** - проверка/создание системы координат (если указана)
3. **Ensure dataset** - проверка/создание датасета (если указано имя)
4. **Create scan** - создание нового скана в БД
5. **Upload raw artifacts** - загрузка артефактов (point cloud, trajectory, control points) в S3
6. **Create ingest run** - создание записи о запуске инжеста
7. **Process ingest run** - обработка: создание манифеста и регистрация derived артефактов

## Мониторинг

Вы можете отслеживать прогресс workflow в Temporal UI:
- URL: http://localhost:18233
- Найдите workflow по ID: `ingest-{company_id}-{dataset_id}-{timestamp}`

## Ошибки

Если что-то пошло не так:
1. Проверьте логи worker'а
2. Проверьте статус workflow в Temporal UI
3. Убедитесь, что все сервисы запущены (Temporal, PostgreSQL, MinIO)
4. Проверьте правильность путей к файлам

