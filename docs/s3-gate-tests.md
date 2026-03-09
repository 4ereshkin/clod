# S3 gate tests (SeaweedFS compatibility)

Цель: быстрый, детерминированный чек S3‑совместимости.

## Подготовка .env

Для SeaweedFS (S3 gateway) задайте:

```
S3_ENDPOINT=http://127.0.0.1:8333
S3_ACCESS_KEY=admin
S3_SECRET_KEY=admin
S3_BUCKET=lidar-data
S3_REGION=us-east-1
```

## Запуск

```
python scripts/s3_gate_test.py
```

### Полезные параметры

- `--large-size-mb 256` — проверить multipart на больших файлах.
- `--multipart-threshold-mb 8` — порог, после которого boto3 включает multipart.
- `--parallel-workers 32 --parallel-rounds 128` — нагрузка на HEAD/GET.

## Настройка порога multipart в приложении

По умолчанию `S3Store` использует single PUT до 8 ГБ (threshold=8GB). Переопределить можно через env:

```
S3_MULTIPART_THRESHOLD_GB=8
```

## Что проверяется

1. PUT → HEAD → GET на малом объекте (байт‑в‑байт).
2. `upload_file()` (multipart) на большом объекте + сверка hash после download.
3. Параллельные HEAD/GET на небольшом объекте.
