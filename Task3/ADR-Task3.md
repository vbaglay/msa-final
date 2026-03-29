# ADR-Task3: Глобальное развёртывание и высокая нагрузка GoFuture

**Статус:** Принято
**Дата:** 2026-03-28

---

## Контекст

GoFuture осуществляет экспансию в Юго-Восточную Азию и Южную Америку. Требуется обеспечить 99,99% доступности, низкую задержку для пользователей в этих регионах, развёртывание более чем в 3 географических регионах, а также отказоустойчивость и соответствие локальным регуляторным требованиям.

В As-Is:
- Нет регионального распределения — работает единая площадка.
- Scaling ручной и вертикальный (увеличение ресурсов EC2).
- Нет глобального traffic management.
- Нет системного DR и failover.

## Требования

- Выбор и обоснование регионов развёртывания.
- Модель глобального развёртывания (active-active / active-passive).
- Схема репликации данных между регионами.
- Механизм геомаршрутизации пользователей.
- План аварийного переключения (failover).
- Security и perimeter protection (WAF, Firewall и аналогичные).
- Стратегия соответствия локальным регуляторным требованиям (compliance).

---

## 1. Выбор регионов развёртывания

### Выбранные регионы

| Регион | Роль | Обоснование |
|---|---|---|
| **Singapore** | Primary hub SEA | Центральная позиция в SEA, зрелая cloud-инфраструктура, низкая задержка по всему региону, хорошая связность |
| **Jakarta** | Local SEA cell | Ближе к Индонезии (крупнейший рынок SEA), улучшает compliance и локальную задержку |
| **São Paulo** | Primary hub South America | Лучший охват Бразилии и основных рынков южноамериканского региона |
| **Santiago** | Secondary South America cell / DR | Низкая задержка для испаноязычных рынков, DR-разделение от São Paulo |

### Оценка

| Критерий | Singapore | Jakarta | São Paulo | Santiago |
|---|---|---|---|---|
| Latency для целевых пользователей | <30ms SEA | <10ms Индонезия | <20ms Бразилия | <20ms Чили/Аргентина |
| Cloud availability | Высокая | Средняя | Высокая | Средняя |
| Compliance (data residency) | Нейтральная | Строгая (Индонезия) | Строгая (LGPD) | Умеренная |
| Стоимость | Выше средней | Средняя | Выше средней | Средняя |

### Альтернативы
1. **3 региона без пар.** Минус: слабее DR, один отказ региона = длительный outage для целого макрорегиона.
2. **Global single region + CDN.** Минус: не решает data residency и latency для stateful операций.
3. **5+ регионов.** Минус: чрезмерная сложность и стоимость для первого года.

---

## 2. Модель глобального развёртывания

### Выбранный вариант: Regional Cells + Active-Active

Каждый регион содержит собственный data plane (cell). Stateless-компоненты работают active-active, stateful — с single-writer per market.

### Распределение по компонентам

| Компонент | Модель | Обоснование |
|---|---|---|
| Edge API / API Gateway | Active-active per region | Stateless, маршрутизация по GeoDNS |
| Booking / Driver / Pricing / Payments / etc. | Active-active per region | Stateless сервисы обрабатывают запросы локально |
| PostgreSQL (transactional) | Single-writer per market/region | Избегаем multi-master, снижаем риск конфликтов |
| Kafka | Региональный кластер + cross-region mirror | Локальная обработка событий, зеркалирование для DR/analytics |
| Redis | Региональный (не реплицируется) | Cache warm-up при failover |
| Elasticsearch | Региональный | Геоиндекс по определению локален |
| ClickHouse | Региональный ingest + глобальная аналитика | Streaming ingest локально, глобальный merge для BI |
| Prometheus / Grafana | Региональные агенты + центральный дашборд | Метрики собираются локально, визуализируются централизованно |
| IAM / Tenant Config | Глобальный control plane + региональные реплики | Конфигурация едина, enforcement локален |

---

## 3. Схема репликации данных

| Тип данных | Primary | Репликация | Consistency Model | Ограничения |
|---|---|---|---|---|
| Транзакционные (bookings, payments, drivers) | Home region market | Async replica → paired region | Strong local, eventual cross-region | Нельзя multi-master; failover через promotion |
| События (Kafka) | Региональный кластер | Kafka MirrorMaker / Cluster Linking → paired region | Ordered per key в регионе | Задержка cross-region 50-200ms |
| Cache (Redis) | Региональный | Не реплицируется | Eventual / none | Cache warm-up при failover |
| Аналитические данные | Региональный ingest | Async merge в глобальный analytical layer | Eventual | Приемлемо для BI/ML |
| Tenant/config | Global control plane | Async с версионированием → региональные реплики | Near-strong для config rollout | Осторожность при изменении политик |

### Описание схемы репликации (для диаграммы)
- Primary PostgreSQL (home region) → async streaming replication → standby replica (paired region)
- Regional Kafka → MirrorMaker 2 / Cluster Linking → paired region Kafka
- Regional analytics ingest → async batch → global ClickHouse layer
- IAM/Tenant Config → read replicas per region

---

## 4. Геомаршрутизация

### Выбранный вариант: GeoDNS + Health-Based Routing + Tenant Policy

**Принцип работы:**
1. Пользователь отправляет DNS-запрос.
2. GeoDNS/GSLB определяет ближайший регион по IP-геолокации.
3. Health checks проверяют доступность региональных endpoints.
4. Compliance policy filter проверяет, допустимо ли обслуживание в данном регионе (data residency tenant).
5. Запрос направляется в ближайший healthy и compliant регион.
6. WAF фильтрует вредоносный трафик до API Gateway.
7. Regional Edge API маршрутизирует запрос к regional services.

**При деградации региона:**
- GSLB помечает регион как unhealthy.
- Трафик перенаправляется в paired region.
- Для stateful операций: failover ограничен home region policy и DR-процедурами.

### Альтернативы
1. **Anycast + BGP.** Минус: сложнее управление, не решает data residency.
2. **CDN-only routing.** Минус: не работает для stateful API.
3. **Client-side region selection.** Минус: зависимость от клиента, сложнее контроль.

---

## 5. Plan аварийного переключения (Failover)

### Сценарии отказа и реакция

| # | Сценарий | Точка отказа | Механизм переключения | RTO | RPO |
|---|---|---|---|---|---|
| 1 | Отказ Edge/API Gateway | Regional ingress | Автоматический reroute через GeoDNS/GSLB | 1-3 мин | 0 |
| 2 | Отказ application tier (один сервис) | Service pod/VM | Autoscaling / redeploy | 1-5 мин | 0 |
| 3 | Отказ Kafka кластера в регионе | Regional broker cluster | Failover consumers на paired region mirror + replay | 15-30 мин | сек-мин |
| 4 | Отказ primary PostgreSQL | DB primary node | Promotion standby replica | 15-30 мин | сек-мин |
| 5 | Полный отказ региона | All regional infrastructure | DR activation: traffic shift + DB promotion + consumer restart | 30-60 мин | мин |
| 6 | Отказ external integration (Yandex Pay, Maps) | External system | Circuit breaker, graceful degradation | — | — |
| 7 | Network partition между регионами | Inter-region link | Autonomous operation per region; reconciliation after recovery | — | мин |

### RTO / RPO целевые

| Уровень | Компоненты | RTO | RPO |
|---|---|---|---|
| Tier 1 (критичный путь) | Booking, Payments, Driver Assignment | <15 мин | <1 мин |
| Tier 2 (важный) | Pricing, Geography, Fraud | <30 мин | <5 мин |
| Tier 3 (допустимая деградация) | Analytics, Notification, BI | <60 мин | <30 мин |

### Механизм переключения
- Автоматический failover для stateless-компонентов (GeoDNS health checks).
- Полуавтоматический failover для stateful (DB promotion по runbook с подтверждением SRE).
- DR game days — регулярные учения аварийного переключения.

---

## 6. Security и Perimeter Protection

### Новые элементы To-Be (относительно As-Is)

| Элемент | Назначение | Уровень |
|---|---|---|
| **WAF** | Фильтрация вредоносного трафика (SQLi, XSS, bot protection) | Edge перед API Gateway |
| **GeoDNS / GSLB** | Глобальная маршрутизация по геолокации и health | DNS/Edge |
| **DDoS Protection** | Защита от volumetric атак | Edge / Cloud provider |
| **API Gateway Rate Limiting** | Защита от abuse и перегрузки | Edge API |
| **Network Segmentation / Firewall** | Изоляция сервисных сетей (public, service mesh, data tier) | VPC / network level |
| **mTLS** | Взаимная аутентификация service-to-service | Service mesh / sidecar |
| **Central IAM / OIDC** | Единая identity management для пользователей, сервисов и партнёров | Platform control plane |
| **Secrets Manager** | Централизованное управление секретами и ротация | Platform |
| **Audit Log Pipeline** | Запись всех admin actions и data access | Platform → analytics |

### Схема уровней защиты
```
Пользователь → GeoDNS → DDoS Protection → WAF → API Gateway (rate limit, auth)
→ Service Mesh (mTLS) → Microservice → DB (encrypted, access-controlled)
```

### Альтернативы
1. **Без WAF, только API rate limiting.** Минус: не защищает от application-level атак.
2. **Service mesh для всего (включая edge).** Минус: overhead, сложность.

---

## 7. Стратегия Compliance

### Принципы
- PII хранится в home region конкретного рынка/tenant.
- Cross-region передаются только operational/aggregated данные без PII.
- Для BI/ML используются masked / tokenized datasets.
- Все admin actions и data access логируются в audit pipeline.
- Tenant policies определяют allowed data residency и failover scope.

### Применение по регионам

| Регион | Ключевые требования | Реализация |
|---|---|---|
| Индонезия (Jakarta) | GR 71/2019: данные пользователей должны храниться локально | DB primary в Jakarta; cross-region только для операционных метрик |
| Бразилия (São Paulo) | LGPD: права субъекта данных, DPO, ограничения на cross-border transfer | PII в São Paulo; consent management; right-to-delete в пайплайне |
| Сингапур | PDPA: уведомление и согласие, ограничения на transfer | Стандартная модель с consent |
| Чили (Santiago) | Ley 19.628 + реформа: data protection, rights of data subjects | PII в Santiago; audit trail |

### Data Residency Architecture
- Каждый tenant привязан к home region.
- Tenant Config Service хранит policy: какие данные где можно хранить.
- Kafka events содержат regionId — routing-ключ для корректного хранения.
- Analytics layer работает с masked/aggregated данными при cross-region merge.

---

## 8. Описание диаграмм

### C2 Multi-Region Deployment
См. [C2-Multi-Region.puml](C2-Multi-Region.puml).

Структурное описание:
- Пользователь → GeoDNS/GSLB → WAF → Regional Edge (Singapore / Jakarta / São Paulo / Santiago)
- Regional Edge → Regional API Gateway → Regional Microservices
- Regional Microservices → Regional PostgreSQL, Regional Kafka, Regional Redis, Regional Elasticsearch
- Regional Observability Agents → Prometheus/Grafana (regional + central dashboards)
- Cross-region: PostgreSQL streaming replication, Kafka MirrorMaker, Config replication

### C4 Failover Diagram
См. [C4-Failover.puml](C4-Failover.puml).

Структурное описание:
- Region A (primary) failure detected by health monitoring
- GeoDNS removes Region A from routing
- Traffic shifts to Region B (paired)
- DB replica promotion in Region B
- Kafka consumers resume from mirrored topics
- Alerts → Alertmanager → SRE инженер → Incident workflow
- After recovery: reconciliation, traffic restore, failback

---

## 9. Компромиссы

- Multi-region увеличивает стоимость инфраструктуры.
- Single-writer ограничивает cross-region write availability.
- Cache не реплицируется — при failover возможно ухудшение latency.
- Compliance требования могут ограничивать гибкость failover.
- Операционная сложность значительно возрастает.

## Риски и митигация

| Риск | Последствие | Митигация |
|---|---|---|
| Split-brain при network partition | Двойная запись, конфликты | Single-writer + fencing tokens |
| Длительный failover для stateful | Превышение RTO | DR runbooks + game days |
| Cache cold start после failover | Всплеск latency | Pre-warming стратегия |
| Non-compliance data leak | Штрафы, потеря доверия | Tenant policies + audit + automated checks |
| Cost explosion | Бюджетный overrun | FinOps dashboards + autoscaling policies |
| Kafka mirror lag | Потеря событий при fast failover | Monitoring mirror lag + acceptable RPO |
