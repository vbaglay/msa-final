# ADR-Task1: Доменная декомпозиция GoFuture

**Дата:** 2026-03-28

---

## Контекст

GoFuture — платформа агрегирования такси с пассажирским и водительским приложениями, корпоративным веб-порталом и внешними интеграциями. Текущая система реализована как монолит на Django с общей PostgreSQL, RabbitMQ, Celery, Redis, Elasticsearch и аналитическим контуром Spark/Flink → ClickHouse → DataLens.

Бизнесу требуется масштабирование до 500 000 конкурентных поездок, работа в нескольких географических регионах, доступность 99,95%+, сокращение time-to-market до 2 недель и поддержка мультитенантности.

Внутри монолита уже выделяются доменные компоненты: Booking, Driver, Pricing, Payments, Payouts, Notification, Geography, Analytics, Fraud. Организационная структура включает продуктовые команды по тем же доменам.

## Требования

- Определить и приоритизировать нефункциональные требования.
- Сформировать карту целевых доменных сервисов.
- Определить очерёдность выделения сервисов из монолита.
- Описать механизм обратной совместимости на переходный период.
- Подготовить план миграции данных.
- Описать диаграмму C2 To-Be.

---

## 1. Нефункциональные требования

| Приоритет | Требование | Целевое значение | Обоснование |
|---|---|---|---|
| P1 | Масштабируемость | ≥ 500 000 конкурентных поездок | Основная бизнес-цель |
| P1 | Доступность | 99,95% базово, 99,99% для критичного пути в multi-region | Критично для поездок и платежей |
| P1 | Низкая задержка | Региональный ответ для booking/pricing/dispatch | Влияет на UX и matching водителей |
| P1 | Отказоустойчивость | Деградация по сервисам без каскадного отказа | Главная проблема монолита |
| P1 | Безопасность | mTLS, IAM, audit, secret rotation | Платежи, PII, партнёры |
| P1 | Compliance | Data residency, auditability | Выход в новые страны |
| P2 | Наблюдаемость | Полные метрики/логи/трассировка/алерты | Нужно для MTTR и SRE |
| P2 | Независимый деплой | Независимый deploy доменов | Цель — 2 недели time-to-market |
| P2 | Консистентность данных | Strong consistency только в локальных транзакциях | Реалистично для MSA |
| P3 | Сопровождаемость | Ownership per team | Соответствие оргструктуре |

---

## 2. As-Is

### Архитектура
- Единый Django-монолит обслуживает все клиентские приложения по HTTP/REST API.
- Единая PostgreSQL RDS — основная БД для всех доменов.
- RabbitMQ + Celery — асинхронная обработка (уведомления, выплаты, аналитика).
- Redis — кеширование (pricing rules, device tokens, геоданные).
- Elasticsearch — геопоиск водителей.
- Prometheus + Grafana + Loki + Alertmanager — наблюдаемость.
- Jenkins + Docker Registry — CI/CD.
- Spark/Flink → ClickHouse → DataLens — аналитика.

### Внешние интеграции
- Яндекс Пэй (платежи, вызывается из Payments Domain).
- Яндекс Карты (геоданные, вызывается из Geography Domain).
- FCM / APNs / Huawei Push Kit (уведомления, вызываются из Notification Tasks).
- API Банка (выплаты, вызывается из Payout Tasks).

### Синхронные зависимости между доменами
- Booking → Driver, Pricing, Payments, Geography, Fraud, Notification
- Driver → Pricing
- Payments → Fraud
- Payouts → Driver
- Geography → Analytics

### Доступ к данным
- Booking → bookings, drivers
- Driver → drivers, payouts
- Pricing → pricing_rules
- Payments → payments
- Payouts → payouts
- Geography → drivers, zones
- Analytics → все таблицы
- Fraud → payments, bookings

### Проблемы As-Is
1. Единая БД создаёт жёсткую связанность и мешает сервисному владению данными.
2. Синхронные междоменные вызовы повышают риск каскадных отказов.
3. Масштабирование возможно только крупными блоками.
4. Релизы замедлены общей кодовой базой и долгими сборками (>30 мин).
5. Организационная структура уже доменная, а техническая архитектура — нет.
6. Переход к multi-region, realtime pricing и мультитенантности неуправляем.

---

## 3. Решение To-Be: Карта доменов и сервисов

Выбран подход эволюционной декомпозиции по уже существующим доменным границам с использованием strangler pattern.

### Обоснование выбора
- Домены уже существуют в монолите и совпадают с продуктовыми командами.
- Strangler pattern минимизирует риск простоя и позволяет мигрировать постепенно.
- Баланс между простотой (не слишком мелкие сервисы) и гибкостью (не слишком крупные).

### Альтернативы
1. **Большой взрыв.** Минус: высокий риск, длительный простой, невозможность параллельной разработки.
2. **Слишком мелкая декомпозиция.** Минус: преждевременный рост операционной сложности.
3. **Один сервис на команду без platform-сервисов.** Минус: слабый control plane и пересекающиеся ответственности.

### Целевая карта сервисов

| Сервис | Команда | Ответственность | Данные | Публикуемые события | Потребляемые события |
|---|---|---|---|---|---|
| Edge API / BFF | Platform | Внешний API, auth, routing, совместимость | — | — | — |
| Booking Service | Booking | Жизненный цикл поездки | bookings, trip state | BookingCreated, BookingCancelled, TripStarted, TripCompleted | PriceQuoted, DriverAssigned, PaymentCaptured, FraudCheckCompleted |
| Driver Service | Driver | Профиль, статус, availability | drivers, status | DriverStatusChanged, DriverLocationUpdated | BookingCreated, DispatchRequested |
| Driver Assignment Service | Driver | Подбор и назначение водителя | assignment state | DriverAssigned, AssignmentTimedOut | BookingCreated, PriceQuoted, DriverLocationUpdated |
| Pricing Service | Pricing | Quote, surge, rules | pricing rules, quotes | PriceQuoted, FareCalculated, SurgeUpdated | BookingCreated, DemandSignalUpdated |
| Payments Service | Payments | Auth/capture/refund | payments, invoices | PaymentAuthorized, PaymentCaptured, PaymentFailed | BookingCreated, TripCompleted, FraudCheckCompleted |
| Payouts Service | Payments | Выплаты водителям | payouts, settlements | PayoutScheduled, PayoutCompleted, PayoutFailed | TripCompleted, PaymentCaptured |
| Notification Service | Notification | Push/email/sms | templates, device tokens | NotificationSent, NotificationFailed | Бизнес-события всех доменов |
| Geography Service | Geography | Маршруты, ETA, geo index | zones, route cache | ETAComputed, DriverProximityUpdated | DriverLocationUpdated, BookingCreated |
| Fraud Service | Fraud | Rules, risk scoring | fraud cases, scores | FraudCheckCompleted, FraudAlertRaised | BookingCreated, PaymentAuthorized |
| Analytics Ingestion | Analytics | Приём доменных событий | event metadata | — | Все доменные события |
| Tenant Config Service | Platform | Tenant config, policies | tenant config | TenantProvisioned | Onboarding events |
| IAM Service | Platform | Authn/authz, SSO | identities, roles | IdentityProvisioned | Onboarding events |

### Трансформация синхронных связей As-Is → To-Be

| Связь As-Is | Тип As-Is | Решение To-Be | Обоснование |
|---|---|---|---|
| Booking → Pricing | sync | sync gRPC (запрос цены — критичный путь) | Цена нужна до подтверждения заказа |
| Booking → Driver | sync | events (DriverAssignmentRequested → DriverAssigned) | Подбор допускает eventual consistency |
| Booking → Payments | sync | events (TripCompleted → PaymentCaptured) | Оплата после поездки, не требует sync |
| Booking → Geography | sync | sync gRPC (ETA — часть UX) | Нужен немедленный ответ |
| Booking → Fraud | sync | events (FraudCheckRequested → FraudCheckCompleted) | Можно выполнить параллельно |
| Booking → Notification | sync | events (side effect) | Уведомления — не критический путь |
| Driver → Pricing | sync | events (FareCalculated) | Расчёт заработка может быть eventual |
| Payments → Fraud | sync | events (FraudCheckRequested → FraudCheckCompleted) | Проверка может быть параллельной |
| Payouts → Driver | sync | events (обогащение через кеш данных водителя) | Водительские данные читаются из локальной реплики |
| Geography → Analytics | sync | events (метрики геопоиска публикуются в Kafka) | Аналитика — always eventual |

---

## 4. Очерёдность выделения сервисов

### Принципы приоритизации
- Минимизация риска простоя.
- Наличие уже выраженной асинхронной или внешней границы.
- Возможность независимого масштабирования.
- Низкая связность с ядром жизненного цикла поездки.

### Roadmap

#### Q1: Foundation + Quick Wins
| Сервис | Действие | Причина |
|---|---|---|
| Edge API | развёртывание | без API gateway миграция неуправляема |
| Observability hardening | расширение Prometheus/Grafana/Loki | необходимо до начала выделения сервисов |
| CI/CD модернизация | pipeline per service | необходимо для independent deploy |
| Notification Service | выделение | уже есть async boundary через Celery |
| Analytics Ingestion | выделение | уже есть отдельный аналитический контур |

#### Q2: Высокоценные домены с чёткими границами
| Сервис | Действие | Причина |
|---|---|---|
| Pricing Service | выделение | чёткая бизнес-граница, критичен для realtime pricing |
| Fraud Service | выделение | можно вынести как decision engine |
| Geography Service | выделение | естественная граница по внешней интеграции и geo-index |

#### Q3: Чувствительные финансовые домены
| Сервис | Действие | Причина |
|---|---|---|
| Payments Service | выделение | высокоценный, хорошо отделим по данным |
| Payouts Service | выделение | отделяется после стабилизации payment events |

#### Q4: Ядро бизнес-процесса
| Сервис | Действие | Причина |
|---|---|---|
| Driver Service | выделение | тесно завязан на booking flow |
| Driver Assignment Service | выделение | зависит от Driver и Geography |
| Booking Service | выделение | самое связанное ядро, переносится последним |
| Монолит | decommission | после переноса всех доменов |

### Quick wins
- Независимый deploy Notification → мгновенная разгрузка монолита от push-логики.
- Независимое масштабирование Pricing → быстрая реакция на surge.
- Переход аналитики на доменные события → повышение качества данных.

### Высокорисковые миграции
- Booking Service — максимальная связность, миграция требует зрелого event-контура.
- Driver Service — пересекается с Booking по данным (drivers, assignment).
- Shared таблицы `bookings` и `drivers` — наибольшая сложность декомпозиции данных.

---

## 5. Механизм обратной совместимости

### Выбранный подход: Strangler + API Gateway + Anti-Corruption Layer

1. Все внешние клиенты продолжают обращаться через **Edge API**.
2. Edge API маршрутизирует каждый endpoint либо в монолит, либо в новый сервис.
3. Для невыделенных доменов сохраняется вызов в монолит.
4. Между монолитом и новыми сервисами используется **ACL** (anti-corruption layer).
5. Запрещается прямой dual-write: монолит не может писать в данные, ownership которых передан сервису.
6. Переключение проводится в 3 шага:
   - **Shadow read** — новый сервис читает, но результат не используется, сравнивается с монолитом.
   - **Write switch** — запись идёт только через новый сервис.
   - **Read switch** — чтение переключается на новый сервис полностью.
7. Feature flags управляют маршрутизацией на каждом шаге.

### Альтернативы
- **Branch by Abstraction.** Минус: сложнее для legacy Django-кода.
- **Parallel Run без ACL.** Минус: высокий риск dual-write и рассинхронизации.

### Компромиссы
- В переходный период архитектура сложнее, чем As-Is или чистое To-Be.
- Требуется дисциплина в управлении feature flags и маршрутизацией.
- Часть сценариев временно проходит через ACL, что добавляет задержку.

---

## 6. План миграции данных

### Подход: Schema extraction first, physical DB split later

| Шаг | Действие | Риск | Контроль |
|---|---|---|---|
| 1 | Зафиксировать логическое владение таблицами по доменам в общей PostgreSQL | Новые зависимости | Контракт на доступ |
| 2 | Ввести outbox pattern и событийную репликацию изменений | Дублирование событий | Idempotency |
| 3 | Backfill сервисных БД из монолита | Неполная миграция | Reconciliation jobs |
| 4 | Переключить запись только через service owner | Блокировка legacy пути | Feature flags |
| 5 | Переключить чтение клиентов на новый сервис | Латентные баги | Canary rollout |
| 6 | Удалить legacy writes и cross-table dependencies | «Забытые» зависимости | Dependency inventory |

### Порядок миграции данных по доменам
1. **Notification:** device tokens, templates, delivery logs — низкая связность с другими доменами.
2. **Pricing:** pricing_rules, quotes, surge factors — изолированные таблицы.
3. **Fraud:** fraud cases, scores — отдельная предметная область.
4. **Geography:** zones, route cache, driver geo index — Redis + Elasticsearch уже отделены.
5. **Payments / Payouts:** payments, payouts, settlements — после стабилизации event flow.
6. **Driver / Booking:** drivers, bookings, trip state — самые связанные, последними.

### Контроль согласованности
- Reconciliation jobs между монолитом и сервисными БД на каждом этапе.
- Мониторинг расхождений через Prometheus + Grafana.
- Alerting на anomalies (пропущенные записи, расхождения count/sum).
- Canary rollout при переключении чтения.

---

## 7. Описание диаграммы C2 To-Be

См. файл [C2-To-Be.puml](C2-To-Be.puml).

### Структурное описание

**Внешний поток:**
- Пассажир → Пассажирское приложение → Edge API → Booking Service
- Водитель → Водительское приложение → Edge API → Driver Service
- Корпоративный менеджер → Корпоративный портал → Edge API → Booking/Payments APIs
- Бухгалтер → Админ-панель → Edge API → Payouts/Operations APIs
- Разработчик → Jenkins
- SRE инженер → Grafana / Alertmanager

**Сервисы и хранилища:**
- Booking Service → Booking DB (PostgreSQL)
- Driver Service → Driver DB (PostgreSQL)
- Driver Assignment Service → Assignment DB (PostgreSQL)
- Pricing Service → Pricing DB (PostgreSQL) + Pricing Cache (Redis)
- Payments Service → Payments DB (PostgreSQL) → Яндекс Пэй
- Payouts Service → Payouts DB (PostgreSQL) → API Банка
- Notification Service → Notification DB (PostgreSQL) → FCM / APNs / Huawei Push Kit
- Geography Service → Geo Cache (Redis) + Geo Index (Elasticsearch) → Яндекс Карты
- Fraud Service → Fraud DB (PostgreSQL)
- Analytics Ingestion → Flink/Spark → ClickHouse → DataLens

**Событийный слой:**
- Все доменные сервисы → Kafka (event backbone)
- RabbitMQ сохраняется как transitional legacy queue

**Переходный слой:**
- Edge API → ACL → GoFuture Monolith (legacy) → Legacy DB (PostgreSQL RDS)

**Observability:**
- Все сервисы → Prometheus → Grafana
- Все сервисы → Loki
- Prometheus → Alertmanager → SRE инженер

---

## 8. Итоги

### Ключевые архитектурные решения
1. Strangler вместо big bang.
2. DB ownership по сервисам.
3. Kafka как backbone, RabbitMQ временно для legacy.
4. Edge API как единая внешняя точка.
5. Single-writer на owned data.
6. Hybrid saga для критичного пути поездки.

### Проблема As-Is → Решение To-Be

| Проблема As-Is | Решение To-Be |
|---|---|
| Единый монолит | Поэтапная декомпозиция по доменам |
| Shared database | Service-owned data с поэтапной миграцией |
| Tight sync coupling | Доменные события + явные API-контракты |
| Ручной scaling | Container platform + autoscaling |
| Долгие релизы (>30 мин сборка) | Independent deploy per service |
| Слабая domain ownership | Service/team alignment |
| Каскадные отказы | Blast-radius reduction + circuit breakers |

### Риски и митигация

| Риск | Последствие | Митигация |
|---|---|---|
| Dual-write при миграции | Рассинхронизация данных | Outbox + single writer + phased cutover |
| Скрытые DB-зависимости | Прод-сбои после выноса сервиса | Dependency inventory + shadow reads |
| Рост операционной сложности | Увеличение MTTR | Observability hardening до начала миграции |
| Ошибки маршрутизации в переходный период | Потеря запросов или двойная обработка | Feature flags + canary rollout |
| Недооценка сложности переноса Booking/Driver | Регрессии в критичном пути | Перенос ядра в последней фазе |
