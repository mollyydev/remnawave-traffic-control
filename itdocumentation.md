# Whitelists Traffic Service

Production-oriented сервис на Go для отдельного учета **whitelists traffic** поверх существующей инфраструктуры Remnawave.

Сервис не поднимает собственные Redis/PostgreSQL контейнеры. На текущем сервере он использует:

- существующий PostgreSQL Remnawave, но отдельную БД `whitelists`;
- существующий Redis Remnawave и Redis Stream `ioraw:export:user_usage`;
- отдельный контейнер только для самого Go-сервиса.

Сервис предназначен для:

- отдельного лимита whitelists для каждого пользователя;
- учета только выбранных Remnawave nodes;
- API для Bedolaga/Subpage/других клиентов;
- автоматической блокировки whitelists squad при достижении лимита;
- автоматического восстановления squad после top-up или reset;
- необязательного принудительного drop текущих соединений;
- необязательного подписанного webhook при исчерпании лимита;
- синхронизации периода учета с Remnawave;
- восстановления учета после перезапусков, потери Redis сообщений и других transient-сбоев.

---

## Архитектура

```text
                         Remnawave
                     ┌───────┴────────┐
                     │                │
                     ▼                ▼
              PostgreSQL          Redis
              database:           Stream:
              remnawave           ioraw:export:user_usage
                     │                │
                     │                ▼
                     │        ┌──────────────────┐
                     │        │ Go Redis Worker  │
                     │        └────────┬─────────┘
                     │                 │
                     ▼                 ▼
              ┌──────────────────────────────┐
              │ PostgreSQL database          │
              │          whitelists          │
              │                              │
              │ whitelist_users              │
              │ processed_stream_messages    │
              │ outbox_events                │
              │ schema_migrations             │
              └──────────────┬───────────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Whitelists API   │
                    │ :8080            │
                    └────────┬─────────┘
                             │
                  ┌──────────┴──────────┐
                  ▼                     ▼
              Bedolaga              Admin/API
```

Redis здесь используется как транспорт stream-событий и consumer-group state. Источник истины для quota и восстановительного учета — PostgreSQL.

Remnawave документирует Redis Streams export для user usage через `ioraw:export:user_usage`; актуальная API также содержит user lookup/update и connections API. См. официальные ссылки в разделе References.

---

# API

Все HTTP endpoints защищены секретным ключом:

```http
X-API-Key: YOUR_SECRET_KEY
```

Ключ сравнивается constant-time и не поддерживается в query string.

## GET `/v1/users/{id}/trafic`

Возвращает:

- `total` — весь выделенный пользователю whitelists traffic в байтах;
- `used` — использованный whitelists traffic в байтах.

```bash
curl \
  -H 'X-API-Key: YOUR_API_KEY' \
  http://127.0.0.1:8080/v1/users/123/trafic
```

Ответ:

```json
{
  "total": 107374182400,
  "used": 28500000000
}
```

`total=0` — безлимитный whitelists quota.

---

## POST `/v1/users/{id}`

Регистрирует локального пользователя в whitelists accounting.

Пример:

```bash
curl -X POST \
  -H 'X-API-Key: YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  http://127.0.0.1:8080/v1/users/123 \
  -d '{
    "total": 100,
    "remnawave_id": 498
  }'
```

Тело:

```json
{
  "total": 100,
  "remnawave_id": 498
}
```

`total` указывается в GB и преобразуется через `1024^3` bytes.

`limit_reset` необязателен. В момент регистрации локальный whitelists period начинается с текущего времени. Поэтому трафик, использованный пользователем до подключения его к whitelists accounting, не списывается из нового лимита. Когда Remnawave создаст новый panel reset boundary, сервис перейдет на эту boundary и дальше синхронизирует периоды с панелью.

### Автоматическая синхронизация с Remnawave

Без `limit_reset` сервис берет из Remnawave:

```text
traffic_limit_strategy
last_traffic_reset_at
created_at
```

и использует границу текущего panel периода.

Пример: если Remnawave сбросит счетчик через 5 дней, whitelists accounting не создает новый период на 30 дней от даты регистрации. Он продолжает текущий panel period и переключается на новую границу, когда Remnawave реально обновит `last_traffic_reset_at`.

Это позволяет избежать дрейфа между двумя счетчиками.

### Собственный период

Можно явно задать reset в днях:

```json
{
  "total": 100,
  "limit_reset": 30,
  "remnawave_id": 498
}
```

В этом режиме Remnawave остается источником `remnawave_id`, но период reset управляется сервисом.

---

## POST `/v1/users/{id}/trafic/reset`

Ручной reset локального whitelists accounting.

```bash
curl -X POST \
  -H 'X-API-Key: YOUR_API_KEY' \
  http://127.0.0.1:8080/v1/users/123/trafic/reset
```

Ответ:

```json
{
  "id": 123,
  "total": 107374182400,
  "used": 0,
  "remaining": 107374182400
}
```

Если включено `ENFORCE_WHITELIST_ACCESS=true`, reset автоматически ставит access-sync. Если до reset пользователь был ограничен, whitelist squad будет восстановлен.

Важно: этот endpoint сбрасывает **локальный whitelists accounting**. Он не вызывает reset traffic в Remnawave API автоматически.

---

## PUT `/v1/users/{id}`

Изменяет quota и/или режим reset.

### Top-up

```bash
curl -X PUT \
  -H 'X-API-Key: YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  http://127.0.0.1:8080/v1/users/123 \
  -d '{"total":200}'
```

Допустим:

```text
было:
total = 100 GB
used  = 100 GB
squad = removed
```

После:

```text
total = 200 GB
used  = 100 GB
```

локальное состояние становится не exhausted, создается desired-state access-sync, и whitelist squad будет автоматически возвращен.

### Важно: top-up без reset не обнуляет used

```text
total = 100
used  = 80

PUT total=200

used = 80
```

Дополнительные 120 GB просто увеличивают доступный остаток.

### Уменьшение лимита

```json
{
  "total": 80
}
```

При `used >= 80` пользователь снова считается exhausted, whitelist squad удаляется при включенном enforcement.

### Custom reset

```json
{
  "limit_reset": 30
}
```

Изменение периода не обнуляет `used`.

### Вернуться к reset Remnawave

```json
{
  "limit_reset": null
}
```

Сервис заново берет актуальные reset strategy/last reset marker из Remnawave и перестраивает usage относительно panel периода.

### Поле `limit_reset` отсутствует

Режим reset не изменяется.

---

## DELETE `/v1/users/{id}`

Удаляет локального пользователя полностью из БД сервиса и удаляет его pending outbox work.

```bash
curl -X DELETE \
  -H 'X-API-Key: YOUR_API_KEY' \
  http://127.0.0.1:8080/v1/users/123
```

Ответ:

```http
204 No Content
```

Remnawave user при этом не удаляется.

Также сервис намеренно не меняет squad membership в рамках `DELETE`: endpoint отвечает за удаление локального accounting state.

---

# Enforcement whitelist squad

Включение:

```env
ENFORCE_WHITELIST_ACCESS=true
WHITELIST_SQUAD_ID=8743a041-b416-4e9f-8588-6d98d4ab1f3e
```

Желаемое состояние:

```text
used < total OR total=0
    -> whitelist squad PRESENT

used >= total
    -> whitelist squad ABSENT
```

Таким образом squad является **desired state**, а не одноразовым действием.

## Top-up после exhaustion

```text
quota exhausted
      ↓
limit_reached=true
      ↓
whitelist squad removed

PUT total=200
      ↓
used < total
      ↓
whitelist.access_sync
      ↓
whitelist squad restored
```

## Reset после exhaustion

```text
quota exhausted
      ↓
whitelist squad removed

POST /trafic/reset
      ↓
used=0
      ↓
whitelist.access_sync
      ↓
whitelist squad restored
```

## Ручной drift

При любом `PUT`/reset access-sync также используется как repair-механизм.

Дополнительно можно включить:

```env
ACCESS_RECONCILE_INTERVAL=10m
```

Тогда сервис периодически будет проверять desired state для локальных пользователей.

По умолчанию interval равен `0`, чтобы не генерировать лишнюю нагрузку на Remnawave API.

---

# Drop активных соединений

По умолчанию удаление squad не считается гарантированным моментальным TCP disconnect для уже открытой сессии.

Чтобы после exhaustion дополнительно попросить Remnawave drop текущие соединения:

```env
ENFORCE_WHITELIST_ACCESS=true
DROP_CONNECTIONS_ON_LIMIT=true
WHITELIST_SQUAD_ID=8743a041-b416-4e9f-8588-6d98d4ab1f3e
```

Сервис сначала приводит squad к отсутствующему состоянию, затем отдельно вызывает Remnawave Connections API.

Операция asynchronous: успешный запрос к Connections API считается принятым, после чего результат фиксируется в PostgreSQL outbox.

`DROP_CONNECTIONS_ON_LIMIT=true` специально требует `ENFORCE_WHITELIST_ACCESS=true`, чтобы пользователь не был отключен от текущей сессии, но тут же получил возможность открыть новую сессию через оставшийся squad.

Для API token нужны соответствующие права Remnawave, включая permission/scope для drop connections.

---

# Webhook

Webhook полностью необязателен.

Включение:

```env
WEBHOOK_ENABLED=true
WEBHOOK_URL=https://example.com/webhooks/whitelists
WEBHOOK_SECRET=VERY_LONG_RANDOM_SECRET
```

Webhook отправляется на переходе:

```text
not exhausted -> exhausted
```

Повторные Redis сообщения после достижения лимита не создают бесконечный поток уведомлений.

Пример body:

```json
{
  "event": "whitelist.exhausted",
  "event_id": "2e9d1c10-...",
  "user_id": 123,
  "remnawave_id": 498,
  "total": 107374182400,
  "used": 107374182400,
  "remaining": 0,
  "generation": 1,
  "occurred_at": "2026-10-04T12:34:56.123Z"
}
```

Headers:

```text
X-Whitelists-Event-ID: 2e9d1c10-...
X-Whitelists-Event: whitelist.exhausted
X-Whitelists-Timestamp: 1791117296
X-Whitelists-Signature: sha256=...
```

Подпись:

```text
HMAC_SHA256(
  WEBHOOK_SECRET,
  timestamp + "." + raw_request_body
)
```

Receiver должен:

1. проверить допустимый clock skew;
2. вычислить HMAC по raw body;
3. сравнить подпись constant-time;
4. дедуплицировать `X-Whitelists-Event-ID`.

Webhook имеет семантику **at-least-once delivery**. Receiver обязан быть идемпотентным.

## Stale webhook protection

Сервис защищает от важной race condition:

```text
limit reached
   ↓
exhaustion event created

почти сразу PUT total=200
   ↓
limit no longer reached
```

Старый webhook не будет отправлен, если событие уже не соответствует актуальному exhaustion state.

Дополнительно используется `exhaustion_generation`, поэтому старое событие не будет повторно считаться актуальным после цикла:

```text
exhausted
   ↓
top-up/reset
   ↓
active
   ↓
new exhaustion
```

---

# Redis Streams

По умолчанию:

```env
REDIS_STREAM=ioraw:export:user_usage
REDIS_CONSUMER_GROUP=whitelists-v1
```

Сервис использует Redis Consumer Group.

Каждое обработанное сообщение дополнительно фиксируется в:

```text
processed_stream_messages(stream_id PRIMARY KEY)
```

Это защищает от повторного начисления одного и того же stream message после reconnect/claim.

Pending сообщения автоматически забираются через `XAUTOCLAIM`, если consumer, который их получил, перестал отвечать.

Если stream был trimmed или сервис долго находился offline, correctness восстанавливается через reconciliation из `nodes_user_usage_history`.

---

# WHITELISTS_NODE_IDS

Сервис считает whitelists traffic только с выбранных числовых Remnawave node IDs:

```env
WHITELISTS_NODE_IDS=1,2
```

Если usage пользователь получил:

```text
node 1 -> 10 GB
node 2 -> 20 GB
node 3 -> 100 GB
```

и:

```env
WHITELISTS_NODE_IDS=1,2
```

whitelists `used` будет:

```text
30 GB
```

Node 3 не учитывается.

---

# Некорректные Redis-сообщения

Если формат входящего Redis Stream сообщения некорректен (например, отсутствует `nodeId`, `ts` или `records`), сервис считает сообщение permanent error, пишет ошибку в лог и ACK-ает его. Это сделано намеренно: бесконечный retry одного сломанного сообщения не должен блокировать recovery/consumer group. Потерянный usage при этом восстанавливается последующей reconciliation из `nodes_user_usage_history`.

---

# Reconciliation

Redis stream является быстрым путем учета, но не единственным механизмом защиты.

Периодически сервис берет authoritative history из Remnawave:

```text
nodes_user_usage_history
```

и пересчитывает current `used_bytes`.

По умолчанию:

```env
RECONCILE_INTERVAL=15m
RECONCILE_SAFETY_MARGIN=2m
```

Safety margin нужен для того, чтобы не считать историческую БД окончательно записанной слишком близко к текущему моменту. Для обычных периодов reconciliation дополнительно не уменьшает `used` внутри уже открытого периода, если historical DB временно отстает от Redis stream: это консервативное поведение не позволяет случайно вернуть whitelist доступ из-за запаздывающей истории. Реальный reset меняет `period_started_at`, после чего счетчик может начаться заново.

Reconciliation также защищает при:

- перезапуске Redis worker;
- временной недоступности Redis;
- trim Redis Stream;
- кратковременной потере stream delivery;
- миграции сервиса;
- восстановлении после crash.

Для стандартных reset strategies используется текущая локальная/panel period boundary.

Для `MONTH_ROLLING` stream deltas напрямую не прибавляются: usage рассчитывается reconciliation как trailing 30-day window.

---

# Reset synchronization с Remnawave

Для `reset_mode=REMNAWAVE` Remnawave является источником истины для panel reset marker.

Используются:

```text
traffic_limit_strategy
last_traffic_reset_at
created_at
```

Поддерживаемые Remnawave стратегии, которые сервис учитывает:

```text
NO_RESET
DAY
WEEK
MONTH
MONTH_ROLLING
```

Сервис не пытается приблизительно вычислять календарный месяц или неделю самостоятельно.

Если до panel reset осталось 5 дней, локальная запись продолжает тот же accounting period. Когда Remnawave продвинет `last_traffic_reset_at`, сервис сбросит local usage и выставит новую boundary.

---

# Concurrency и production safety

## 1. Top-up race

События exhaustion являются transition events, а не командами "удалить squad навсегда".

Перед внешней операцией worker снова читает актуальный user state.

Поэтому:

```text
exhaustion event
      ↓
PUT total
      ↓
enforcement worker
```

не должен удалить squad после успешного top-up, если quota уже снова доступна.

## 2. Generation race

Каждый переход в `exhausted` получает собственный `exhaustion_generation`.

Старое событие от предыдущего exhaustion cycle не может повторно сработать после нового top-up/reset и нового exhaustion cycle.

## 3. Concurrent reconciliation

Reconciliation использует optimistic concurrency check: если во время SQL-подсчета изменились period/reset mode/baseline или `updated_at` пользователя, старый результат не перезаписывает новое состояние. Это защищает от затирания usage, которое пришло из Redis stream одновременно с reconciliation.

## 4. Outbox

Внешние действия не выполняются как часть HTTP request.

Сначала атомарно меняется PostgreSQL state:

```text
quota state
+
outbox event
```

После commit отдельный worker выполняет:

```text
Remnawave PATCH
Remnawave connections/drop
Webhook
```

Если процесс падает после внешнего запроса, outbox повторит безопасную операцию.

## 5. Access sync как desired state

`whitelist.access_sync` не хранит "добавь squad" или "удали squad".

Worker каждый раз вычисляет желаемое состояние из текущей quota:

```text
used < total -> present
used >= total -> absent
```

Поэтому один и тот же механизм используется для:

- top-up;
- reset;
- startup repair;
- manual drift correction;
- восстановления после временного сбоя Remnawave API.

---

# PostgreSQL

Используется отдельная БД внутри уже существующего PostgreSQL сервера:

```text
PostgreSQL server
├── remnawave
└── whitelists
```

Рекомендуется:

```text
whitelists              -> read/write user
remnawave                -> отдельный read-only user
```

### Основные таблицы

```text
whitelist_users
processed_stream_messages
outbox_events
schema_migrations
```

### Автоматические миграции

Сервис сам применяет embedded migrations при старте.

Используется transaction-scoped PostgreSQL advisory lock, поэтому параллельный запуск нескольких реплик не должен одновременно выполнять одну и ту же migration.

Ручные `.sql` файлы в `migrations/` остаются в проекте как reference/emergency migration path.

---

# Установка

## Требования

- Docker;
- Docker Compose v2;
- существующий Remnawave PostgreSQL;
- существующий Remnawave Redis;
- Docker network `remnawave-network`;
- Redis Stream export в Remnawave;
- Remnawave API token с необходимыми правами.

---

## 1. Проверить Docker network

```bash
docker network inspect remnawave-network
```

Если сеть уже существует — ничего делать не нужно.

---

## 2. Включить Redis Streams export Remnawave

В `.env` Remnawave:

```env
EXPORT_TO_STREAM_ENABLED=true
EXPORT_TO_STREAM_MAXLEN=3000
```

После изменения параметров recreate/restart контейнеров Remnawave.

Проверить stream:

```bash
docker exec -it remnawave-redis redis-cli XINFO STREAM ioraw:export:user_usage
```

Посмотреть последние сообщения:

```bash
docker exec -it remnawave-redis redis-cli XREVRANGE ioraw:export:user_usage + - COUNT 3
```

Если у Redis есть пароль, добавьте параметры auth.

---

## 3. Создать отдельную БД `whitelists`

Узнай имя существующего PostgreSQL container. Ниже пример:

```text
remnawave-db
```

Подключение:

```bash
docker exec -it remnawave-db psql -U postgres
```

Создать role + database:

```sql
CREATE USER whitelists WITH PASSWORD 'CHANGE_ME_STRONG_PASSWORD';
CREATE DATABASE whitelists OWNER whitelists;

CREATE USER whitelists_reader WITH PASSWORD 'CHANGE_ME_READER_PASSWORD';
```

Если они уже существуют — повторно создавать не нужно.

Права для собственной БД:

```sql
GRANT ALL PRIVILEGES ON DATABASE whitelists TO whitelists;
```

После подключения к `whitelists`:

```sql
GRANT ALL ON SCHEMA public TO whitelists;
ALTER SCHEMA public OWNER TO whitelists;
```

Read-only роль для Remnawave:

```sql
GRANT CONNECT ON DATABASE remnawave TO whitelists_reader;
```

Затем:

```sql
\c remnawave

GRANT USAGE ON SCHEMA public TO whitelists_reader;
GRANT SELECT ON TABLE users, nodes_user_usage_history TO whitelists_reader;
```

Поменяй names/passwords под свою установку.

---

## 4. Настроить `.env`

```bash
cp .env.example .env
nano .env
```

Минимальный production-вариант:

```env
HTTP_ADDR=:8080
API_KEY=GENERATE_A_LONG_RANDOM_SECRET

DATABASE_URL=postgresql://whitelists:CHANGE_ME_STRONG_PASSWORD@remnawave-db:5432/whitelists
DB_MAX_CONNS=4

REMNAWAVE_DATABASE_URL=postgresql://whitelists_reader:CHANGE_ME_READER_PASSWORD@remnawave-db:5432/remnawave
REMNAWAVE_DB_MAX_CONNS=4

REDIS_ADDR=remnawave-redis:6379
REDIS_PASSWORD=
REDIS_DB=0
REDIS_POOL_SIZE=8
REDIS_STREAM=ioraw:export:user_usage
REDIS_CONSUMER_GROUP=whitelists-v1
REDIS_CONSUMER_NAME=
REDIS_START_ID='$'

WHITELISTS_NODE_IDS=1,2

REMNAWAVE_URL=http://remnawave:3000
REMNAWAVE_TOKEN=YOUR_REMNAWAVE_API_TOKEN
REMNAWAVE_HTTP_TIMEOUT=15s
REMNAWAVE_MAX_RETRIES=3

REMNAWAVE_SYNC_INTERVAL=2m
RECONCILE_INTERVAL=15m
RECONCILE_SAFETY_MARGIN=2m
ACCESS_RECONCILE_INTERVAL=0
WORKER_POLL_INTERVAL=1s
OUTBOX_CLAIM_LEASE=2m
OUTBOX_MAX_RETRIES=12
RECONCILE_PAGE_SIZE=250
REMNAWAVE_SYNC_BATCH_SIZE=500
PROCESSED_MESSAGE_RETENTION=35d
OUTBOX_RETENTION=30d
MIGRATION_TIMEOUT=60s
SHUTDOWN_TIMEOUT=15s

ENFORCE_WHITELIST_ACCESS=true
WHITELIST_SQUAD_ID=8743a041-b416-4e9f-8588-6d98d4ab1f3e
DROP_CONNECTIONS_ON_LIMIT=false

WEBHOOK_ENABLED=false
WEBHOOK_URL=
WEBHOOK_SECRET=
WEBHOOK_TIMEOUT=10s
WEBHOOK_MAX_RETRIES=12
```

### API key

```bash
openssl rand -hex 32
```

Минимум 32 символа.

### Webhook secret

```bash
openssl rand -hex 64
```

Минимум 32 символа при `WEBHOOK_ENABLED=true`.

---

## 5. Build / start

```bash
docker compose up -d --build
```

Проверить:

```bash
docker compose ps
docker compose logs -f whitelists-service
```

Внутри Docker network Bedolaga может обращаться к API через:

```text
http://whitelists-service:8080
```

На хосте текущий compose публикует API только локально:

```text
http://127.0.0.1:8080
```

---

# Первичная проверка

Рекомендуемый первый запуск:

```env
ENFORCE_WHITELIST_ACCESS=false
DROP_CONNECTIONS_ON_LIMIT=false
WEBHOOK_ENABLED=false
```

Зарегистрировать тестового пользователя:

```bash
curl -i -X POST \
  -H 'X-API-Key: YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  http://127.0.0.1:8080/v1/users/123 \
  -d '{"total":1,"remnawave_id":498}'
```

Проверить usage:

```bash
curl -s \
  -H 'X-API-Key: YOUR_API_KEY' \
  http://127.0.0.1:8080/v1/users/123/trafic
```

Сравнить с Remnawave DB:

```sql
SELECT
    h.user_id,
    COALESCE(SUM(h.total_bytes), 0) AS total_bytes
FROM nodes_user_usage_history h
WHERE h.user_id = 498
  AND h.node_id IN (1,2)
GROUP BY h.user_id;
```

После проверки включить:

```env
ENFORCE_WHITELIST_ACCESS=true
```

И протестировать:

```text
registration
    ↓
usage
    ↓
exhaustion
    ↓
squad removed
    ↓
PUT total выше used
    ↓
squad restored
    ↓
POST reset
    ↓
squad restored
```

---

# Healthcheck

`/healthz` также защищен API key.

```bash
curl -i \
  -H 'X-API-Key: YOUR_API_KEY' \
  http://127.0.0.1:8080/healthz
```

Ответ:

```json
{"status":"ok"}
```

Health проверяет доступность:

- whitelists PostgreSQL;
- Redis;
- Remnawave PostgreSQL.

---

# Security

Сервис запускается в Docker:

- не от root;
- с `read_only: true`;
- `cap_drop: ALL`;
- `no-new-privileges`;
- с ограничением памяти `256m`;
- с CPU limit `0.50`;
- без записи API secret в image.

API key передается только через HTTP header.

Webhook secret не используется для API authentication.

Remnawave DB рекомендуется подключать отдельным read-only role.

---

# Масштабирование

Текущий compose предназначен для одной реплики.

Архитектура consumer group позволяет позже масштабировать worker горизонтально:

```text
whitelists-1 ─┐
whitelists-2 ─┼── Redis Consumer Group ── PostgreSQL
whitelists-3 ─┘
```

При масштабировании:

- все реплики используют один `REDIS_CONSUMER_GROUP`;
- каждая реплика должна иметь уникальный `REDIS_CONSUMER_NAME`;
- лучше оставить `REDIS_CONSUMER_NAME=` пустым, чтобы имя строилось из hostname контейнера;
- общий host port `127.0.0.1:8080:8080` нужно заменить на reverse proxy/internal load balancer, потому что несколько реплик не могут одновременно занять один published port.

Перенос на отдельный сервер не требует переписывать код: меняются connection env vars.

---

# Перенос Redis/PostgreSQL позже

Сейчас:

```text
SERVER 1
├── Remnawave
├── Remnawave Redis
├── PostgreSQL
├── whitelists DB
└── Whitelists Service
```

Позже:

```text
SERVER 1
├── Remnawave
├── Remnawave Redis
└── PostgreSQL

SERVER 2
├── Whitelists Service
└── dedicated PostgreSQL/Redis (optional)
```

Основная конфигурация остается:

```env
DATABASE_URL=...
REMNAWAVE_DATABASE_URL=...
REDIS_ADDR=...
```

---

# Что происходит после полного расхода

Сценарий по умолчанию при включенном enforcement:

```text
Redis usage delta
      ↓
PostgreSQL used_bytes
      ↓
used >= total
      ↓
limit_reached=true
      ↓
outbox: whitelist.exhausted
      ↓
Remnawave PATCH
      ↓
remove whitelist squad
```

Если включен drop:

```text
remove squad
      ↓
Remnawave connections/drop
```

Если включен webhook:

```text
whitelist.exhausted
      ↓
HMAC webhook
```

Все внешние операции имеют retry/backoff и не блокируют API request.

---

# Важные семантики

## `total=0`

`0` означает unlimited whitelist quota.

## `used` может быть больше `total`

Это возможно из-за сетевой/stream propagation задержки или уже установленной активной сессии после достижения лимита.

Например:

```text
total = 100 GB
used  = 103 GB
```

Состояние все равно exhausted.

После top-up:

```text
PUT total=200
```

получим:

```text
used  = 103 GB
total = 200 GB
remaining = 97 GB
```

и squad будет восстановлен.

## Повторный top-up

`PUT` является setter-операцией:

```json
{"total":200}
```

не добавляет 200 GB к текущему лимиту, а устанавливает новый общий лимит `200 GB`.

Если нужен additive top-up, клиент должен сначала получить текущий `total`, затем отправить новое значение.

---

# Troubleshooting

## Redis stream не существует

Проверь:

```bash
docker exec -it remnawave-redis redis-cli XINFO STREAM ioraw:export:user_usage
```

и Remnawave:

```env
EXPORT_TO_STREAM_ENABLED=true
```

## Пользователь не учитывается

Проверь:

```text
WHITELISTS_NODE_IDS
remnawave_id
nodes_user_usage_history
```

## Squad не удаляется

Проверь:

```env
ENFORCE_WHITELIST_ACCESS=true
WHITELIST_SQUAD_ID=...
REMNAWAVE_TOKEN=...
```

и:

```bash
docker compose logs -f whitelists-service
```

## Squad не возвращается после top-up

Проверь локально:

```bash
curl -s \
  -H 'X-API-Key: YOUR_API_KEY' \
  http://127.0.0.1:8080/v1/users/123/trafic
```

Если:

```text
used < total
```

то сервис должен иметь pending `whitelist.access_sync` или уже выполнить его.

Также проверь `ENFORCE_WHITELIST_ACCESS=true`.

## Webhook не приходит

Проверь:

```env
WEBHOOK_ENABLED=true
WEBHOOK_URL=https://...
WEBHOOK_SECRET=...
```

Потом логи:

```bash
docker compose logs -f whitelists-service
```

Webhook создается только на переходе в exhausted state, а stale exhaustion events после top-up/reset намеренно подавляются.

---

# Сборка без Docker

Требуется Go 1.23+.

```bash
go mod download
go build -trimpath -ldflags='-s -w' -o whitelists ./cmd/whitelists
```

Запуск:

```bash
./whitelists
```

Полезные команды:

```bash
make fmt
make vet
make test
```

---

# Структура проекта

```text
whitelists-service/
├── cmd/
│   └── whitelists/
│       └── main.go
├── internal/
│   ├── api/
│   │   ├── server.go
│   │   └── server_test.go
│   ├── config/
│   │   └── config.go
│   ├── remna/
│   │   └── remna.go
│   ├── store/
│   │   ├── migrate.go
│   │   ├── store.go
│   │   └── migrations/
│   ├── stream/
│   │   ├── consumer.go
│   │   └── consumer_test.go
│   └── webhook/
│       └── webhook.go
├── migrations/
│   ├── 001_init.sql
│   ├── 002_production_safety.sql
│   ├── 003_outbox_dedup.sql
│   ├── 004_connection_drop.sql
│   └── 005_exhaustion_generation.sql
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── openapi.yaml
├── Makefile
├── .gitignore
└── README.md
```

---

# References

- Remnawave API: https://docs.rw/api/
- Remnawave environment variables / Redis Streams export: https://docs.rw/install/environment-variables/
- Remnawave users: https://docs.rw/learn-en/users/
- Remnawave v3 migration notes: https://f.docs.rw/t/topic/354
