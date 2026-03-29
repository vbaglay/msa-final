# ADR-Task4: Data Platform, ML-интеграция и BI

**Статус:** Принято
**Дата:** 2026-03-28

---

## Контекст

GoFuture нуждается в единой платформе для сбора, обработки и анализа данных из всех микросервисов. Платформа должна поддерживать ML-модели динамического ценообразования, прогнозирования спроса и обнаружения мошенничества, а также обеспечивать BI-инструменты для бизнес-аналитики.

В As-Is уже существует аналитический контур:
- События из приложения/воркеров поступают в Analytics Engine (Spark/Flink).
- Данные записываются в ClickHouse.
- Визуализация выполняется через DataLens.

## Требования

- Создать схему сбора данных из всех микросервисов.
- Интегрировать data pipeline с ML-платформой.
- Интегрировать data pipeline с BI-инструментами.
- Описать механизмы обеспечения качества данных.

---

## As-Is аналитического контура

- GoFuture Monolith и Celery Workers отправляют события в Analytics Engine (Spark/Flink) через AMQP.
- Analytics Engine записывает данные в ClickHouse через ETL процесс.
- DataLens визуализирует бизнес-метрики из ClickHouse.
- Корпоративный менеджер получает бизнес-отчёты, бухгалтер — финансовые отчёты.

### Проблемы As-Is
1. Нет единого event contract — данные поступают ad hoc.
2. Аналитика зависит от монолита и его модели данных.
3. Слабая прозрачность: нет lineage, нет data quality checks.
4. ML use cases описаны бизнесом, но не оформлены платформенно.
5. Нет разделения raw/curated layers.
6. Нет feature pipelines для ML-моделей.

---

## Решение To-Be: Event-Driven Data Platform

### Выбранный вариант
Эволюция текущего аналитического контура в event-driven data platform с разделением на raw/curated/serving layers, feature pipelines для ML и data contracts.

### Обоснование
- Текущий контур (Spark/Flink → ClickHouse → DataLens) уже работает и может быть развит.
- Event-driven подход обеспечивает единый источник данных из всех микросервисов.
- Разделение на layers обеспечивает масштабируемость и governance.

### Альтернативы
1. **Чисто batch-подход.** Минус: не закрывает realtime pricing/fraud.
2. **Чисто streaming-подход.** Минус: неудобен для обучения моделей и исторической аналитики.
3. **Полностью новая data platform без переиспользования.** Минус: дороже и дольше, игнорирует рабочие инструменты.

---

## 1. To-Be Data Pipeline

### Архитектура по слоям

| Слой | Компоненты | Назначение |
|---|---|---|
| **Sources** | Все доменные микросервисы | Публикация бизнес-событий через Kafka |
| **Ingestion** | Analytics Ingestion Service | Нормализация, валидация, routing событий |
| **Raw Storage** | Object Storage / Kafka long-term | Хранение сырых событий без трансформации |
| **Stream Processing** | Apache Flink | Realtime aggregates, features, anomaly signals |
| **Batch Processing** | Apache Spark | Исторические витрины, training datasets, reconciliation |
| **Curated Storage** | ClickHouse | Витрины данных, агрегаты, feature tables |
| **Serving** | ClickHouse + Feature Store | BI queries, ML feature delivery |
| **BI** | DataLens | Дашборды и отчёты |
| **ML Platform** | MLflow / Kubeflow (допущение) | Training, registry, deployment, monitoring моделей |

### Data Flow

| # | Источник | Получатель | Тип | Описание |
|---|---|---|---|---|
| 1 | Доменные сервисы | Kafka | Streaming | Публикация бизнес-событий |
| 2 | Kafka | Analytics Ingestion | Streaming | Валидация и нормализация |
| 3 | Analytics Ingestion | Raw Storage | Streaming | Сохранение сырых событий |
| 4 | Analytics Ingestion | Flink | Streaming | Потоковая обработка |
| 5 | Flink | ClickHouse | Streaming | Realtime агрегаты и features |
| 6 | Flink | Feature Store | Streaming | Online features для ML |
| 7 | Raw Storage | Spark | Batch | Исторический анализ |
| 8 | Spark | ClickHouse | Batch | Curated витрины |
| 9 | Spark | Feature Store | Batch | Offline features для ML |
| 10 | ClickHouse | DataLens | Query | BI-отчёты |
| 11 | Feature Store | ML Scoring Services | Query | Feature delivery |
| 12 | ML Scoring Services | Pricing/Fraud/Forecasting | gRPC/REST | Inference results |

---

## 2. ML Use Cases

### Dynamic Pricing
- **Входные данные:** BookingCreated, DriverLocationUpdated, SurgeUpdated, исторические паттерны спроса.
- **Streaming features:** текущий спрос по зонам, доступность водителей, средний ETA.
- **Batch features:** исторические коэффициенты по времени суток, дню недели, сезону.
- **Модель:** предсказывает оптимальный surge factor.
- **Потребитель:** Pricing Service.

### Demand Forecasting
- **Входные данные:** исторические trip-данные, география, временные паттерны, внешние факторы (события, погода — допущение).
- **Batch features:** агрегаты по зонам и временным окнам.
- **Модель:** предсказывает спрос на горизонте 15 мин — 24 часа.
- **Потребитель:** Driver Assignment (supply planning), Pricing (proactive surge).

### Fraud Detection
- **Входные данные:** BookingCreated, PaymentAuthorized, паттерны поведения (device, account, payment).
- **Streaming features:** частота операций, суммы, device fingerprints.
- **Batch features:** исторический risk profile.
- **Модель:** risk scoring в реальном времени.
- **Потребитель:** Fraud Service.

---

## 3. BI и витрины данных

| Витрина | Потребитель | Режим | Источник |
|---|---|---|---|
| Ride Operations Mart | Operations / Product | Near real-time | Booking, Driver, Geography events |
| Pricing Effectiveness Mart | Pricing team | Near real-time + daily | Pricing, Booking events |
| Driver Supply/Utilization Mart | Driver / Operations | Near real-time | Driver, Dispatch events |
| Payment & Payout Mart | Finance / Accountant | Near real-time + daily close | Payment, Payout events |
| Partner/Tenant Performance Mart | Partner Management | Daily + hourly | Tenant, Booking, Payment events |
| Fraud Investigation Mart | Fraud team | Near real-time | Fraud, Payment, Booking events |

---

## 4. Механизмы обеспечения качества данных

| Механизм | Назначение | Реализация |
|---|---|---|
| **Freshness SLA** | Контроль задержек ingestion | Мониторинг lag по consumer groups, alert при превышении порога |
| **Completeness Checks** | Обнаружение потерь событий | Сравнение count событий в Kafka vs ClickHouse за окно |
| **Schema Registry** | Контроль формата событий | Avro/Protobuf schemas, backward compatibility checks |
| **Data Contracts** | Обязательные поля и ownership | Каждый домен публикует контракт на свои события |
| **Lineage Metadata** | Прослеживаемость происхождения данных | Метаданные в каждом событии: source, version, timestamp |
| **Schema Evolution** | Безопасное изменение формата | Backward-compatible changes по умолчанию, breaking — новая версия |
| **Reconciliation Jobs** | Сверка ключевых бизнес-сущностей | Периодическая проверка: booking ↔ payment ↔ payout |
| **Anomaly Detection** | Обнаружение аномалий в потоках | Статистический контроль объёмов и паттернов |
| **Quality Dashboards** | Видимость для data/platform teams | Дашборды в Grafana: freshness, completeness, errors |
| **Access Audit** | Контроль доступа к данным | Логирование всех BI/ML-запросов к sensitive data |

---

## 5. Описание диаграммы C4 Data Pipeline

См. файл [C4-Data-Pipeline.puml](C4-Data-Pipeline.puml).

### Структурное описание

- Booking Service → Kafka → Analytics Ingestion
- Driver Service → Kafka → Analytics Ingestion
- Pricing Service → Kafka → Analytics Ingestion
- Payments Service → Kafka → Analytics Ingestion
- Payouts Service → Kafka → Analytics Ingestion
- Fraud Service → Kafka → Analytics Ingestion
- Analytics Ingestion → Raw Storage (Object Storage)
- Analytics Ingestion → Flink (streaming processing)
- Flink → ClickHouse (realtime aggregates)
- Flink → Feature Store (online features)
- Raw Storage → Spark (batch processing)
- Spark → ClickHouse (curated marts)
- Spark → Feature Store (offline features)
- Feature Store → ML Scoring Services
- ML Scoring Services → Pricing Service, Fraud Service, Forecasting
- ClickHouse → DataLens (BI queries)
- DataLens → Корпоративный менеджер, Бухгалтер, Аналитик

---

## 6. Компромиссы

- Dual path (streaming + batch) сложнее, чем один из подходов.
- Feature Store — дополнительный компонент инфраструктуры.
- Data contracts требуют дисциплины от всех доменных команд.
- ML Platform (MLflow/Kubeflow) — допущение; конкретный выбор зависит от зрелости команды.

## Риски и митигация

| Риск | Последствие | Митигация |
|---|---|---|
| Data swamp (сырые данные без структуры) | Невозможность использовать данные | Разделение raw/curated, data contracts |
| Schema chaos | Падение pipelines | Schema Registry + CI checks |
| Hidden bias в ML-моделях | Некорректные pricing/fraud-решения | Model monitoring, drift detection, governance |
| BI на устаревших данных | Некорректные бизнес-решения | Freshness SLA + alerting |
| Tenant data leakage в analytics | Нарушение изоляции | Tenant-aware filters, masked marts |
| Pipeline failure cascades | Потеря данных | DLQ, replay, reconciliation |
