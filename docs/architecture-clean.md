# Clean architecture target (current refactor)

## Layers

- `lidar_app.app.domain` — pure domain entities and rules.
- `lidar_app.app.crs_normalizer` — transport-agnostic CRS normalization core.
- `lidar_app.app.application` — orchestration/use-cases (to be expanded).
- `lidar_app.app.infrastructure` — integrations (db, s3, messaging adapters).
- `lidar_app.app.interfaces` — external API/transport entrypoints (RabbitMQ/SignalR adapters).

## CRS normalization boundary

CRS normalizer accepts and returns only CRS-related models:

- input: `CRSNormalizeRequestV1`
- output: `CRSNormalizeResultV1`

No business metadata (company, plan, department, etc.) is allowed in this boundary.

## Messaging direction

RabbitMQ and SignalR should be thin adapters that map transport payloads
into `CRSNormalizeRequestV1`, call the application/core, and map back responses.

## Immediate follow-up checklist

1. Add transport adapters in `interfaces/` for RabbitMQ and SignalR that only map transport payloads to `CRSNormalizeRequestV1`.
2. Keep CRS normalization logic in `crs_normalizer` only; do not duplicate in transport/persistence code.
3. Move any remaining exploratory scripts out of production import paths.
4. Add integration tests for adapter -> service -> normalized result path.
