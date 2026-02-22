# Ревью ingest (application + interfaces)

## Общая оценка

Архитектура ingest-потока в целом выстроена правильно (DTO -> command -> use case -> Temporal -> status/events), но в текущей реализации есть несколько критичных дефектов, которые могут привести к ошибкам рантайма и рассинхронизации контрактов сообщений.

## Критичные замечания

1. **Некорректная инициализация timestamp (фиксируется в момент импорта модуля)**
   - В `ScenarioResult.timestamp`, `StatusEventDTO.timestamp` и `WorkflowFailedDTO.failed_at` используется `time()` прямо в поле модели.
   - Такое значение вычисляется один раз при импорте файла, а не при создании каждого экземпляра.
   - Рекомендуется использовать `Field(default_factory=time)`.

2. **Несоответствие enum/string при маппинге статуса**
   - В `to_status_event` передаётся `status=status.value` (строка), при этом `StatusEventDTO.status` типизирован как `WorkflowStatus`.
   - Pydantic это может неявно сконвертировать, но теряется строгая типизация и прозрачность контракта.

3. **Баг маппинга `etag` в completed event**
   - В `to_completed_event` поле `etag` заполняется значением `s3_key`.
   - Должно быть `etag=item.etag`.

4. **Непоследовательное представление статуса в контрактах**
   - `StatusEventDTO.status` — `WorkflowStatus`, а `WorkflowCompletedDTO.status`/`WorkflowFailedDTO.status` — `Literal[WorkflowStatus.*]`.
   - При этом в `to_completed_event`/`to_failed_event` передаётся enum, а в `to_status_event` — строка.
   - Нужна единая политика: внутри домена работать с enum, сериализацию в строку выполнять только на границе транспорта.

5. **Пустые адаптеры интерфейсного слоя**
   - `interfaces/ingest/consumer.py`, `interfaces/ingest/publisher_contract.py`, `interfaces/rabbit.py`, `interfaces/signalr.py` пустые.
   - Фактически отсутствуют рабочие транспортные реализации для входа/выхода ingest-потока.

6. **Риск KeyError в маппинге результатов**
   - `to_result_objects` использует прямой доступ по ключам (`item['kind']`, `item['s3_key']`) для внешних данных.
   - При невалидном payload это приведёт к исключению; лучше добавить явную валидацию перед конвертацией.

## Замечания средней важности

1. **Реестр сценариев захардкожен внутри функции**
   - В `resolve_scenario` registry определён прямо в теле функции.
   - Для масштабирования по версиям лучше вынести в модульную константу или конфигурацию.

2. **В use case нет явного fail-path**
   - В `StartIngestUseCase.execute` нет orchestration-обработки ошибок (`try/except`) с публикацией `FAILED` статуса/события при сбоях вызовов Temporal.
   - Enum `ErrorCode` уже есть, но не используется в оркестрации.

3. **Стиль mutable default в DTO**
   - В `ScanPayloadDTO` для dict-полей используются `{}`.
   - Для читаемости и единообразия лучше заменить на `Field(default_factory=dict)`.

## Рекомендации (по приоритету)

1. Исправить все timestamp-поля на `default_factory=time`.
2. Исправить маппинг `etag` в `to_completed_event`.
3. Нормализовать работу со статусами: enum внутри домена, строка только на транспортной границе.
4. Добавить fail-path в `StartIngestUseCase.execute` с публикацией failed status/event.
5. Реализовать адаптеры в `interfaces/*` (или убрать заглушки из продового потока).
6. Добавить контрактные тесты для mapper-ов и happy/failure сценариев use case.
