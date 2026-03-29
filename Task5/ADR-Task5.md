# ADR-Task5: Мультитенантная платформа GoFuture

**Статус:** Принято
**Дата:** 2026-03-28

---

## Контекст

GoFuture стремится быстро запускать решения для партнёров в новых регионах с полной изоляцией данных и кастомизацией функциональности. Требуется мультитенантная платформа, которая поддерживает партнёров с различными уровнями изоляции, единую систему управления доступом и автоматизированный процесс подключения.

В As-Is:
- Мультитенантная модель не реализована.
- IAM-система не выделена в отдельный компонент.
- Нет автоматизированного onboarding-процесса.
- Нет tenant-aware мониторинга.

## Требования

- Модель изоляции данных между партнёрами.
- IAM-система с ролевым доступом и SSO.
- Автоматизированный процесс подключения новых партнёров (onboarding).
- Система мониторинга мультитенантного окружения.
- Таблица ролей и уровней доступа.

---

## 1. Модель мультитенантности

### Выбранный вариант: Tiered Multitenancy

Единая модель для всех партнёров не оптимальна: одни требуют жёсткой изоляции (регулируемые рынки, крупные партнёры), другие — стандартного shared-подхода. Поэтому предлагается **tiered-модель** с двумя уровнями.

| Уровень | Модель | Для кого | Изоляция данных | Compute |
|---|---|---|---|---|
| **Standard** | Shared application tier + schema-per-tenant в общей БД | Стандартные партнёры | Logical (schema/row-level) | Shared |
| **Premium / Regulated** | Shared application tier + database-per-tenant | Крупные или регулируемые партнёры | Physical (separate DB) | Shared или dedicated |

### Распределение по компонентам

| Компонент | Shared / Isolated | Описание |
|---|---|---|
| Edge API / API Gateway | Shared | Tenant-aware routing по tenant ID из токена |
| Microservices (Booking, Driver, etc.) | Shared | Tenant context обязателен в каждом запросе |
| PostgreSQL | Tiered | Standard: schema-per-tenant; Premium: DB-per-tenant |
| Kafka | Shared | TenantId в metadata каждого события |
| Redis | Shared | Key prefix per tenant |
| Elasticsearch | Shared | Index-per-tenant или tenant filter |
| ClickHouse | Shared | Tenant-aware filter в витринах |
| IAM | Shared (global) | Единая identity platform |
| Tenant Config | Shared (global) | Tenant policies и config management |
| Monitoring | Shared с tenant-level partitioning | Метрики и дашборды по tenant |

### Альтернативы
1. **Shared-schema-for-all (row-level filtering).** Минус: слабая изоляция, риск утечки данных между партнёрами.
2. **DB-per-tenant для всех.** Минус: дорого и сложно при большом количестве мелких партнёров.

### Компромиссы
- Tiered-модель сложнее единой, но обеспечивает гибкость и контроль затрат.
- Schema-per-tenant может усложнить миграции БД.
- Tenant context пронизывает все слои, что требует дисциплины.

---

## 2. IAM

### Выбранный вариант: Central OIDC/SAML IAM + RBAC + Policy Engine

**Компоненты IAM:**
- **Identity Provider (IdP):** централизованный OIDC/SAML-совместимый сервис.
- **SSO:** поддержка Single Sign-On для партнёров через SAML/OIDC federation.
- **RBAC:** ролевая модель доступа.
- **Policy Engine:** проверка прав на уровне tenant и ресурса.
- **Service Accounts:** machine-to-machine identity для межсервисных вызовов.
- **Audit Log:** все действия по управлению доступом логируются.

### Описание ролей

| Роль | Scope | Описание | Типичный актор |
|---|---|---|---|
| Platform Admin | Platform-wide | Управление lifecycle тенантов, глобальные политики, audit | Команда GoFuture |
| Partner Admin | Конкретный tenant | Управление пользователями, конфигурацией, интеграциями внутри tenant | Администратор партнёра |
| Corporate Manager | Tenant / корп. группа | Управление корпоративными поездками и отчётами | Корпоративный менеджер |
| Finance / Accountant | Tenant | Просмотр финансов, выплат, формирование отчётов | Бухгалтер партнёра |
| Operations Manager | Tenant / регион | Dispatch oversight, операционные дашборды | Операционный менеджер |
| Analyst | Tenant | Доступ к BI-витринам и аналитическим данным | Аналитик |
| Support | Tenant (ограниченный) | Read-only доступ для устранения проблем | Служба поддержки |
| Service Account | Service / tenant | Machine-to-machine действия с минимальными правами | Автоматизация |

### Таблица ролей и уровней доступа к данным

| Роль | Tenant Config | Booking Data | Payment Data | Driver Data | Analytics/BI | User Management | Audit Logs |
|---|---|---|---|---|---|---|---|
| Platform Admin | Full CRUD | Read (all tenants) | Read (all tenants) | Read (all tenants) | Full | Full | Full |
| Partner Admin | Read/Update (own tenant) | Full (own tenant) | Read (own tenant) | Read (own tenant) | Full (own tenant) | Full (own tenant) | Read (own tenant) |
| Corporate Manager | — | Read/Create (own corp) | Read (own corp) | — | Read (own corp) | — | — |
| Finance/Accountant | — | Read (own tenant) | Full (own tenant) | Read payouts | Read financial | — | Read financial |
| Operations Manager | — | Read (own tenant/region) | — | Read (own tenant/region) | Read operational | — | — |
| Analyst | — | — | — | — | Read curated | — | — |
| Support | — | Read (masked PII) | Read (masked) | Read (masked) | — | — | — |
| Service Account | Per policy | Per policy | Per policy | Per policy | Per policy | — | — |

---

## 3. Onboarding новых партнёров

### Автоматизированный flow

| Шаг | Действие | Компонент | Результат |
|---|---|---|---|
| 1 | Partner Admin регистрируется через Partner Portal | Partner Portal | Заявка на создание tenant |
| 2 | Platform Admin утверждает заявку | IAM + Tenant Config | Создание tenant record |
| 3 | IAM provisioning | IAM Service | Создание identity, ролей, SSO-конфигурации |
| 4 | Tenant Config provisioning | Tenant Config Service | Создание tenant policy (tier, region, isolation level) |
| 5 | Infrastructure provisioning | Provisioning Pipeline | DB/schema, Kafka topics, Redis prefixes, secrets |
| 6 | Observability setup | Observability Pipeline | Tenant-specific dashboards, alerts, log routing |
| 7 | Integration setup | Integration Config | Подключение локальных платёжных/карт/push сервисов |
| 8 | Readiness checks | Automated Tests | Health check, data isolation check, monitoring check |
| 9 | Tenant activation | Tenant Config Service | TenantProvisioned event, партнёр активен |

### События onboarding

| Событие | Producer | Consumers |
|---|---|---|
| TenantRegistrationRequested | Partner Portal | Tenant Config |
| TenantApproved | Tenant Config | IAM, Provisioning |
| IdentityProvisioned | IAM | Tenant Config |
| InfrastructureProvisioned | Provisioning Pipeline | Observability, Integration |
| TenantReadinessChecked | Automated Tests | Tenant Config |
| TenantProvisioned | Tenant Config | All services |

---

## 4. Мониторинг мультитенантного окружения

### Tenant-Level Metrics

| Метрика | Назначение |
|---|---|
| Requests per tenant | Capacity planning и billing |
| Error rate per tenant | Здоровье отдельного tenant |
| p95 latency per tenant | Обнаружение noisy neighbor |
| Event lag per tenant/region | Здоровье streaming-обработки |
| Storage consumption per tenant | Контроль квот |
| Spend per tenant | FinOps |
| Auth failures per tenant | Безопасность |
| Policy violations per tenant | Compliance |
| Active users per tenant | Business KPI |
| Onboarding pipeline status | Operational readiness |

### Noisy Neighbor Detection
- Мониторинг p95 latency по tenant.
- Alert при превышении порога одним tenant, влияющем на других.
- Rate limiting per tenant на Edge API.
- Quotas на storage и compute per tenant.

### Дашборды
- **Platform Dashboard:** все tenants, aggregate health, capacity.
- **Tenant Dashboard:** per-tenant metrics, SLA compliance, incidents.
- **Onboarding Dashboard:** pipeline status, pending approvals, readiness.
- **Security Dashboard:** auth failures, policy violations, audit events.

---

## 5. Описание диаграмм

### C2: IAM + Onboarding + Monitoring
См. файл [C2-Multitenancy.puml](C2-Multitenancy.puml).

**Структурное описание:**
- Partner Admin → Partner Portal → IAM Service → Identity provisioning
- Partner Portal → Tenant Config Service → Provisioning Pipeline → tenant resources
- Tenant User → Edge API (tenant-aware routing) → Microservices → tenant-scoped DB/schema
- Microservices → Kafka (tenantId metadata) → Analytics, Monitoring
- Observability Stack → dashboards/alerts partitioned by tenant and region
- Platform Admin → Platform Admin Console → IAM/Tenant Config/Monitoring

### C3 Onboarding Flow (в текстовом виде)
1. Partner Admin → Partner Portal (регистрация)
2. Partner Portal → Tenant Config Service (создание tenant)
3. Tenant Config Service → IAM Service (identity + roles + SSO)
4. Tenant Config Service → Provisioning Pipeline:
   - → DB provisioning (schema или database)
   - → Kafka topic provisioning (tenant metadata)
   - → Redis prefix provisioning
   - → Secrets provisioning
5. Provisioning Pipeline → Observability Setup (dashboards, alerts, log routing)
6. Provisioning Pipeline → Integration Config (payments, maps, push)
7. Automated Readiness Checks → Tenant Config Service
8. Tenant Config Service → TenantProvisioned event → все сервисы

---

## 6. Компромиссы

- Tiered isolation сложнее единой модели, но оправдана для разных категорий партнёров.
- Tenant context пронизывает весь стек, что увеличивает требования к разработке.
- Provisioning pipeline требует инвестиций в автоматизацию.
- SSO federation может требовать индивидуальной настройки для каждого крупного партнёра.

## Риски и митигация

| Риск | Последствие | Митигация |
|---|---|---|
| Data leakage между tenants | Нарушение изоляции, потеря доверия | Schema/DB-level isolation + automated tests |
| Noisy neighbor | Деградация SLA для других партнёров | Rate limiting + quotas + dedicated tier |
| Slow onboarding | Потеря партнёров | Automated pipeline + SLA на onboarding |
| Complexity explosion | Сложность эксплуатации | Tiered model (не всё isolated) |
| IAM misconfiguration | Unauthorized access | Policy-as-code + audit + tests |
| Tenant-specific bugs | Сложная отладка | Tenant-aware logs + tracing |
