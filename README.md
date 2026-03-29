# MSA-GoFuture

Архитектурный проект миграции платформы **GoFuture** от монолитной к микросервисной, событийной и мультирегиональной архитектуре.

## Цели проекта

- Обеспечить масштабирование до **500 000** конкурентных поездок.
- Повысить доступность критичных сервисов до **99,95–99,99%**.
- Сократить time-to-market новых функций до **2 недель**.
- Поддержать realtime pricing, anti-fraud, ML и партнёрскую мультитенантную модель.
- Обеспечить горизонтальное масштабирование в **3+ географических регионах**.
- Соблюсти локальные регуляторные требования (data residency, compliance).

## Структура репозитория

| Директория | Содержимое |
|---|---|
| [`Task1/`](Task1/) | Доменная декомпозиция, NFR, C2 To-Be, roadmap миграции, план миграции данных |
| [`Task2/`](Task2/) | Event-driven архитектура, доменные события, Kafka topic design, мониторинг событийной платформы |
| [`Task3/`](Task3/) | Multi-region deployment, репликация, geo-routing, failover, compliance |
| [`Task4/`](Task4/) | Data platform, ML/BI pipeline, data quality controls |
| [`Task5/`](Task5/) | Multitenancy, IAM, onboarding партнёров, tenant-aware monitoring |

## Артефакты по задачам

### Task1 — Проектируем домены
- [ADR-Task1.md](Task1/ADR-Task1.md) — ADR: нефункциональные требования, карта сервисов, очерёдность выделения, план миграции данных, механизм обратной совместимости
- [C2-To-Be.puml](Task1/C2-To-Be.puml) — диаграмма C2 целевых сервисов

### Task2 — Проектируем Event-Driven архитектуру
- [ADR-Task2.md](Task2/ADR-Task2.md) — ADR: событийная платформа, доменные события, topic design, Saga, надёжная доставка, подход к мониторингу, выбранные инструменты
- [C2-Event-Platform.puml](Task2/C2-Event-Platform.puml) — диаграмма C2 событийной платформы с отражением инструментов мониторинга

### Task3 — Обеспечиваем высокую нагрузку
- [ADR-Task3.md](Task3/ADR-Task3.md) — ADR: выбор регионов, репликация, геомаршрутизация, failover, security, compliance
- [C2-Multi-Region.puml](Task3/C2-Multi-Region.puml) — диаграмма C2 мультирегионального развёртывания
- [C4-Failover.puml](Task3/C4-Failover.puml) — диаграмма C4 аварийного переключения

### Task4 — Data platform / ML / BI
- [ADR-Task4.md](Task4/ADR-Task4.md) — ADR: data pipeline, ML, BI, data quality
- [C4-Data-Pipeline.puml](Task4/C4-Data-Pipeline.puml) — диаграмма C4 пайплайна данных с интеграцией в ML и BI

### Task5 — Мультитенантная платформа
- [ADR-Task5.md](Task5/ADR-Task5.md) — ADR: мультитенантность, IAM, onboarding, мониторинг, таблица ролей
- [C2-Multitenancy.puml](Task5/C2-Multitenancy.puml) — диаграмма C2/C3 с IAM, onboarding и мониторингом

## Логика решения

Исходная система — Django-монолит с общей PostgreSQL, RabbitMQ/Celery, Redis, Elasticsearch и аналитическим контуром Spark/Flink → ClickHouse → DataLens. Целевое решение строится эволюционно через strangler migration: выделение сервисов по существующим доменным границам, введение Kafka как backbone для доменных событий, переход к региональным cell-based deployment units и добавление tenant-aware control plane.

### Ключевые архитектурные решения
- Декомпозиция по уже существующим доменам внутри монолита.
- Strangler pattern вместо big-bang переписи.
- Отказ от shared database в пользу service ownership.
- Kafka как event backbone + RabbitMQ временно для legacy Celery.
- Active-active региональная платформа с single-writer per market.
- Tiered multitenancy для баланса cost/isolation.
- Эволюция существующего observability-стека (Prometheus, Grafana, Loki, Alertmanager).
