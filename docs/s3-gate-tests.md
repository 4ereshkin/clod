# S3 gate tests (SeaweedFS/MinIO compatibility)

Цель: быстрый, детерминированный чек S3‑совместимости перед переключением endpoint.

## Подготовка .env

Для SeaweedFS (S3 gateway) задайте:

```
S3_ENDPOINT=http://127.0.0.1:8333
S3_ACCESS_KEY=admin
S3_SECRET_KEY=admin
S3_BUCKET=lidar-data
S3_REGION=us-east-1
```

Если не заданы `S3_*`, то используется MinIO (`MINIO_PORT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`).

## Запуск

```
python scripts/s3_gate_test.py
```

### Полезные параметры

- `--large-size-mb 256` — проверить multipart на больших файлах.
- `--multipart-threshold-mb 8` — порог, после которого boto3 включает multipart.
- `--parallel-workers 32 --parallel-rounds 128` — нагрузка на HEAD/GET.

## Что проверяется

1. PUT → HEAD → GET на малом объекте (байт‑в‑байт).
2. `upload_file()` (multipart) на большом объекте + сверка hash после download.
3. Параллельные HEAD/GET на небольшом объекте.

## Troubleshooting (Windows)

Если скрипт падает без traceback (например, `exit code 0xC06D007F`), чаще всего это проблема нативных DLL
в окружении Python. Проверьте базовые импорты:

```
python -X faulthandler -c "import boto3; import botocore; print(boto3.__version__)"
```

Если импорт падает, убедитесь, что:

1. Используется то же окружение, где установлены зависимости из `requirements.txt`.
2. Установлены системные компоненты (обычно **Microsoft Visual C++ Redistributable**).
3. В случае Conda окружений — что `python` запускается именно из активированного env.
