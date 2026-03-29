# ADR-Task2: Event-Driven архитектура GoFuture

**Статус:** Принято
**Дата:** 2026-03-28

---

## Контекст

Для обработки 500 000 конкурентных поездок, динамического ценообразования в реальном времени, anti-fraud и масштабируемой аналитики GoFuture требуется событийная платформа. В текущем состоянии асинхронность реализована через RabbitMQ + Celery и ориентирована на выполнение фоновых задач, а не на хранение и обработку доменных событий.

## Требования

- Определить модель доменных событий.
- Спроектировать схему топиков с региональным партиционированием.
- Выбрать подход к realtime-обработке событий.
- Определить тип Saga для распределённых транзакций.
- Описать механизмы надёжной доставки.
- Сформировать подход к мониторингу событийной архитектуры.
- Отразить инструменты мониторинга на C2-диаграмме.

---

## As-Is

- RabbitMQ используется как брокер сообщений для Celery.
- Booking, Payments, Payouts, Analytics отправляют задачи в очередь.
- Отдельные Celery Tasks: Notification Tasks, Payout Tasks, Analytics Tasks.
- Notification Tasks работают с FCM, APNs, Huawei Push Kit.
- Payout Tasks работают с API Банка.
- Аналитический контур: Spark/Flink → ClickHouse → DataLens.

### Проблемы As-Is
1. Нет единого реестра доменных событий и контрактов.
2. Нет replay и stream processing на уровне платформы.
3. RabbitMQ решает локальные task-queue задачи, не образует event backbone.
4. Аналитика и ML не получают единообразный поток бизнес-событий.
5. Наблюдаемость брокера и consumers недостаточна для критичной event-driven архитектуры.

---

## Решение To-Be

### Выбранный вариант: Kafka как event backbone

**Обоснование выбора Kafka:**
- Масштабируемая потоковая обработка с партиционированием по регионам.
- Удобный replay для дебага, восстановления и перестройки состояния.
- Единый источник событий для аналитики, ML и межсервисного взаимодействия.
- Зрелая экосистема для schema registry, connect, streams.

**Место RabbitMQ в целевой архитектуре:**
- Остаётся временно для legacy Celery Tasks.
- По мере выноса сервисов задачи мигрируют на Kafka consumers.
- Milestones вывода RabbitMQ привязаны к roadmap декомпозиции (Task1).

**Альтернативы:**
1. Оставить только RabbitMQ. Минус: хуже для event log, replay, масштабной аналитики.
2. Полностью синхронная архитектура. Минус: не соответствует нагрузке и realtime use cases.

---

## 1. Архитектура событийной платформы

### Producer/Consumer Flow
1. Доменные сервисы записывают изменения в свою БД + outbox-таблицу в одной транзакции.
2. Outbox relay (CDC или polling) публикует события в Kafka.
3. Kafka хранит события в партиционированных топиках.
4. Consumer-сервисы подписываются на нужные топики.
5. Flink/Kafka Streams выполняют realtime-обработку потоков.
6. Analytics Ingestion направляет события в ClickHouse для BI.

### Компоненты
- Kafka cluster (региональный, с mirror/link для cross-region).
- Schema Registry (Avro/Protobuf).
- Outbox relay per service.
- Flink cluster для stream processing.
- Kafka Connect для интеграции с ClickHouse и другими хранилищами.

---

## 2. Модель доменных событий

| # | Событие | Producer | Consumers | Payload summary | Partition Key | Бизнес-смысл |
|---|---|---|---|---|---|---|
| 1 | BookingCreated | Booking | Pricing, Fraud, Dispatch, Notification, Analytics | bookingId, riderId, regionId, origin, destination | regionId#bookingId | Старт запроса поездки |
| 2 | BookingValidated | Booking | Pricing, Dispatch | bookingId, validationResult | regionId#bookingId | Заказ прошёл проверку |
| 3 | BookingCancelled | Booking | Payments, Notification, Analytics | bookingId, reason | regionId#bookingId | Отмена заказа |
| 4 | PriceQuoteRequested | Booking | Pricing | bookingId, route, time | regionId#bookingId | Запрос расчёта цены |
| 5 | PriceQuoted | Pricing | Booking, Analytics | bookingId, fare, surgeFactor | regionId#bookingId | Цена рассчитана |
| 6 | SurgeUpdated | Pricing | Analytics, Geography | regionId, zoneId, surgeFactor | regionId#zoneId | Изменение коэффициента спроса |
| 7 | FraudCheckRequested | Booking/Payments | Fraud | entityId, actorData | regionId#entityId | Запрос проверки риска |
| 8 | FraudCheckCompleted | Fraud | Booking, Payments, Analytics | entityId, riskScore, decision | regionId#entityId | Решение по риску |
| 9 | FraudAlertRaised | Fraud | Analytics, Notification | entityId, alertType | regionId#entityId | Обнаружена подозрительная активность |
| 10 | DriverLocationUpdated | Driver | Geography, Dispatch, Analytics | driverId, coords, ts | regionId#driverId | Обновление геопозиции |
| 11 | DriverStatusChanged | Driver | Dispatch, Analytics | driverId, newStatus | regionId#driverId | Смена статуса водителя |
| 12 | DriverAssignmentRequested | Booking | Dispatch | bookingId, constraints | regionId#bookingId | Начало подбора водителя |
| 13 | DriverAssigned | Dispatch | Booking, Notification, Analytics | bookingId, driverId | regionId#bookingId | Водитель назначен |
| 14 | DriverAssignmentTimedOut | Dispatch | Booking, Pricing, Notification | bookingId, reason | regionId#bookingId | Таймаут подбора |
| 15 | TripStarted | Booking | Pricing, Analytics, Notification | bookingId, tripStartTs | regionId#bookingId | Поездка началась |
| 16 | TripCompleted | Booking | Payments, Payouts, Analytics, Notification | bookingId, tripEndTs, distance | regionId#bookingId | Поездка завершена |
| 17 | FareCalculated | Pricing | Payments, Booking | bookingId, finalFare | regionId#bookingId | Итоговая стоимость |
| 18 | PaymentAuthorized | Payments | Booking, Analytics | paymentId, bookingId, amount | regionId#paymentId | Оплата авторизована |
| 19 | PaymentCaptured | Payments | Booking, Payouts, Analytics | paymentId, bookingId, amount | regionId#paymentId | Деньги списаны |
| 20 | PaymentFailed | Payments | Booking, Notification, Fraud | paymentId, reason | regionId#paymentId | Ошибка оплаты |
| 21 | RefundIssued | Payments | Booking, Notification, Analytics | paymentId, refundAmount | regionId#paymentId | Возврат средств |
| 22 | PayoutScheduled | Payouts | Notification, Analytics | payoutId, driverId, amount | regionId#payoutId | Выплата запланирована |
| 23 | PayoutCompleted | Payouts | Driver, Analytics, Notification | payoutId, status | regionId#payoutId | Выплата завершена |
| 24 | PayoutFailed | Payouts | Notification, Analytics | payoutId, reason | regionId#payoutId | Ошибка выплаты |
| 25 | NotificationRequested | Booking/Payments/Payouts | Notification | target, template, context | regionId#targetId | Запрос уведомления |
| 26 | NotificationSent | Notification | Analytics | notificationId, channel, status | regionId#notificationId | Уведомление доставлено |

---

## 3. Topic Design

| Topic | Назначение | Partition Key | Partitions | Retention | Retry/DLQ |
|---|---|---|---|---|---|
| gf.booking.events.v1 | Lifecycle бронирования | regionId#bookingId | по числу регионов × запас | 14 дней | gf.booking.retry.v1, gf.booking.dlq.v1 |
| gf.driver.events.v1 | Статус и локация водителя | regionId#driverId | по числу регионов × запас | 3 дня (location), 14 дней (state) | gf.driver.retry.v1, gf.driver.dlq.v1 |
| gf.pricing.events.v1 | Quotes, fares, surge | regionId#bookingId | по числу регионов × запас | 7 дней | gf.pricing.retry.v1, gf.pricing.dlq.v1 |
| gf.payment.events.v1 | Payment state | regionId#paymentId | по числу регионов × запас | 90 дней | gf.payment.retry.v1, gf.payment.dlq.v1 |
| gf.payout.events.v1 | Payout state | regionId#payoutId | по числу регионов × запас | 90 дней | gf.payout.retry.v1, gf.payout.dlq.v1 |
| gf.fraud.events.v1 | Risk decisions | regionId#entityId | по числу регионов × запас | 30 дней | gf.fraud.retry.v1, gf.fraud.dlq.v1 |
| gf.notification.events.v1 | Notification requests/results | regionId#targetId | по числу регионов × запас | 7 дней | gf.notification.retry.v1, gf.notification.dlq.v1 |
| gf.dispatch.events.v1 | Assignment events | regionId#bookingId | по числу регионов × запас | 7 дней | gf.dispatch.retry.v1, gf.dispatch.dlq.v1 |
| gf.analytics.events.v1 | Unified analytics feed | regionId#entityId | по числу регионов × запас | 30 дней | replay topics |

### Naming convention
`gf.<domain>.events.v<version>`

### Schema Evolution
- Avro или Protobuf + Schema Registry.
- Backward-compatible changes по умолчанию.
- Breaking changes только через новую версию топика.
- Все producers и consumers проверяются на совместимость при CI/CD.

---

## 4. Realtime Processing

### Dynamic Pricing
- Входные потоки: BookingCreated, DriverLocationUpdated, SurgeUpdated, DemandSignalUpdated.
- Обработка: Flink считает realtime-признаки спроса и предложения по зонам.
- Выход: PriceQuoted, SurgeUpdated → Pricing Service и Analytics.

### Driver Allocation
- Входные потоки: BookingCreated, PriceQuoted, DriverLocationUpdated, DriverStatusChanged.
- Обработка: Driver Assignment Service формирует candidate list на основе позиции, доступности и оптимизации загрузки.
- Выход: DriverAssigned или DriverAssignmentTimedOut.

### Fraud Detection
- Входные потоки: BookingCreated, PaymentAuthorized, паттерны поведения пользователей.
- Обработка: Fraud Service рассчитывает risk score.
- Выход: FraudCheckCompleted, FraudAlertRaised.

### Analytics
- Все доменные события → Analytics Ingestion → Flink (streaming aggregates) + Spark (batch marts) → ClickHouse → DataLens.

---

## 5. Saga

### Выбранный тип: Hybrid Saga

**Обоснование:**
- Чистая хореография усложняет контроль критичного бизнес-процесса.
- Чистая оркестрация создаёт bottleneck и overloading одного компонента.
- Гибридный подход: оркестрация для критичного пути (booking → pricing → fraud → dispatch → payment → payout), хореография для side effects (notification, analytics).

**Альтернативы:**
1. Чистая choreography. Минус: сложнее аудит и контроль.
2. Чистая orchestration. Минус: bottleneck на orchestrator.

### Пример: жизненный цикл бронирования

| Шаг | Событие | Сервис | Действие |
|---|---|---|---|
| 1 | BookingCreated | Booking | Создание заказа, публикация события |
| 2 | PriceQuoted | Pricing | Расчёт стоимости |
| 3 | FraudCheckCompleted | Fraud | Оценка риска |
| 4 | DriverAssigned | Dispatch | Назначение водителя |
| 5 | TripStarted | Booking | Начало поездки |
| 6 | TripCompleted | Booking | Завершение поездки |
| 7 | FareCalculated | Pricing | Итоговый расчёт |
| 8 | PaymentCaptured | Payments | Списание средств |
| 9 | PayoutScheduled | Payouts | Начисление водителю |

### Compensating Actions

| Триггер | Компенсация |
|---|---|
| PaymentFailed | BookingCancelled или переход в статус ошибки |
| DriverAssignmentTimedOut | Повторный подбор или отмена заказа |
| FraudCheckCompleted (rejected) | Блокировка capture, уведомление пассажира |
| PayoutFailed | PayoutRetryScheduled + алерт финансам |

### Consistency Model
- Strong consistency: только внутри одного сервиса (локальная транзакция + outbox).
- Eventual consistency: между сервисами через Kafka events.
- Reconciliation: периодические сверки booking ↔ payment ↔ payout.

---

## 6. Надёжная доставка

| Механизм | Описание |
|---|---|
| Transactional Outbox | Бизнес-изменение и событие записываются в одной транзакции. Relay публикует из outbox в Kafka. |
| Idempotent Consumers | Каждое событие содержит eventId. Consumer проверяет, обработано ли оно ранее. |
| Retry with Backoff | Exponential backoff при ошибках обработки. Ограниченное число попыток. |
| Dead Letter Queue | После исчерпания retry событие попадает в DLQ. DLQ мониторится и разбирается вручную или автоматически. |
| At-Least-Once | Базовая модель доставки для всех доменных событий. |
| Exactly-Once | Используется только в stream processing (Flink), где технически поддерживается. Не делается глобальной целью. |
| Reconciliation Jobs | Периодическая сверка критических бизнес-состояний (booking → payment → payout). |

---

## 7. Подход к мониторингу

### Принцип
Текущий observability-стек (Prometheus, Grafana, Loki, Alertmanager) сохраняется и расширяется. Добавляются exporters и dashboards для Kafka, Flink, consumers и schema registry.

### Метрики событийной платформы

| Метрика | Зачем |
|---|---|
| Consumer lag (по группам) | Обнаружение backlog |
| Publish latency | Проблемы с broker |
| End-to-end event latency | SLA realtime-обработки |
| Failed consumes rate | Стабильность consumers |
| DLQ rate | Проблемы с контрактами или приложением |
| Outbox backlog | Застрявшие publishers |
| Rebalance count | Нестабильность consumer groups |
| Partition skew | Неравномерная нагрузка |
| Schema compatibility failures | Контроль контрактов |
| Flink job restart count | Надёжность stream jobs |
| Broker under-replicated partitions | Здоровье кластера |
| Broker disk saturation | Ёмкость хранения |

### Алерты

| Алерт | Порог | Серьёзность |
|---|---|---|
| Consumer lag > threshold | >10 000 messages для critical consumers | Critical |
| DLQ burst | >100 events/min | High |
| Publish error rate | >1% | High |
| Flink job unhealthy | restart count >3 за 10 мин | Critical |
| Partition under-replication | >0 | High |
| Broker disk >80% | threshold | Warning |
| Schema incompatibility | any | High |
| Outbox backlog growing | >1000 unsent | Warning |
| End-to-end latency >SLA | >5s для critical path | Critical |

### Выбранные инструменты мониторинга

| Инструмент | Назначение | Статус |
|---|---|---|
| Prometheus | Метрики сервисов, брокеров, consumers, stream jobs | Существующий |
| Grafana | Визуализация метрик, дашборды | Существующий |
| Loki | Логи сервисов и consumers | Существующий |
| Alertmanager | Управление алертами, routing по командам | Существующий |
| Kafka Exporter | Метрики Kafka кластера (lag, throughput, partitions) | Новый |
| Flink Metrics Reporter | Метрики stream processing jobs | Новый |
| Schema Registry Metrics | Ошибки совместимости, usage | Новый |

Все инструменты отражены на диаграмме [C2-Event-Platform.puml](C2-Event-Platform.puml).

---

## 8. Описание диаграммы C2 Event Platform

См. файл [C2-Event-Platform.puml](C2-Event-Platform.puml).

### Структурное описание

- Booking Service → Outbox → Kafka gf.booking.events.v1
- Driver Service → Outbox → Kafka gf.driver.events.v1
- Pricing Service → consumes booking events → publishes gf.pricing.events.v1
- Fraud Service → consumes booking/payment events → publishes gf.fraud.events.v1
- Driver Assignment → consumes booking/pricing/driver events → publishes gf.dispatch.events.v1
- Payments Service → consumes trip events → publishes gf.payment.events.v1
- Payouts Service → consumes payment events → publishes gf.payout.events.v1
- Notification Service → consumes business events → FCM / APNs / Huawei Push Kit
- Analytics Ingestion → consumes all business topics → Flink → ClickHouse → DataLens
- Legacy services → RabbitMQ → Celery Tasks (transitional)
- Kafka/Flink/Consumers → Prometheus → Grafana
- Kafka/Flink/Consumers → Loki
- Prometheus → Alertmanager → SRE инженер

---

## 9. Компромиссы

- В переходный период одновременно работают Kafka и RabbitMQ.
- Event-driven подход требует строгой дисциплины в schema management.
- Часть сценариев переходит на eventual consistency.
- Сложность отладки и операционного управления возрастает.
- Stream processing (Flink) добавляет дополнительную инфраструктуру.

## Риски и митигация

| Риск | Последствие | Митигация |
|---|---|---|
| Event storm | Перегрузка consumers | Quotas, backpressure, partition planning |
| Schema drift | Падение consumers | Schema Registry + compatibility checks в CI |
| Дублирование событий | Двойная обработка | Idempotency keys |
| Скрытая sync связь через events | Хрупкость | Contract-first event design |
| Сложность миграции с RabbitMQ | Долгий переходный период | Milestones вывода RabbitMQ |
| DLQ overflow | Потеря бизнес-событий | Мониторинг + автоматический разбор DLQ |
