# Правила хранения в S3

## Общая схема префикса скана

Функция `scan_prefix(...)` строит базовый префикс для артефактов скана:

```
tenants/{company_id}/dataset_versions/{dataset_version_id}/scans/{scan_id}
```

Перед подстановкой идентификаторы нормализуются через `safe_segment(...)`, чтобы префикс был безопасным для S3.

## Нормализация `safe_segment(...)`

`safe_segment(...)` выполняет:

1. Unicode-нормализацию (`NFKC`) и `strip()`.
2. Замену всех символов, не входящих в `[a-zA-Z0-9._-]`, на `_`.
3. Удаление ведущих/замыкающих `_`.
4. Если сегмент пустой — возвращается `na`.

Эта нормализация используется в `scan_prefix(...)` для `company_id`, `dataset_version_id`, `scan_id`, чтобы гарантировать корректные сегменты пути.

## Raw-ключи

Raw-артефакты формируются поверх `scan_prefix(...)`:

- `raw_cloud_key(prefix, filename)` → `{prefix}/raw/point_cloud/{filename}`
- `raw_path_key(prefix)` → `{prefix}/raw/trajectory/path.txt`
- `raw_control_point_key(prefix)` → `{prefix}/raw/control_points/ControlPoint.txt`

## Derived-ключи и пути

Общий формат derived-пути включает версию схемы:

```
{prefix}/derived/v{schema_version}/...
```

### Ключ ingest-манифеста

- `derived_manifest_key(prefix, schema_version)` → `{prefix}/derived/v{schema_version}/ingest_manifest.json`

### Derived-пути, формируемые в activity

- Reprojected point cloud:
  - `{prefix}/derived/v{schema_version}/reprojected/point_cloud/{filename}`
- Reprojected trajectory:
  - `{prefix}/derived/v{schema_version}/reprojected/trajectory/path.txt`
- Reprojected control points:
  - `{prefix}/derived/v{schema_version}/reprojected/control_points/ControlPoint.txt`
- Preprocessed point cloud:
  - `{prefix}/derived/v{schema_version}/preprocessed/point_cloud/{filename}`
- Registration anchors:
  - `{prefix}/derived/v{schema_version}/registration/anchors.json`
- Registration edges:
  - `{prefix}/derived/v{schema_version}/registration/edges_proposed.json`
- Merged point cloud (export):
  - `{prefix}/derived/v{schema_version}/merged/point_cloud/{filename}`
