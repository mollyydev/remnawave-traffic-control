# Remnawave Traffic Control (Whitelists Service)

Сервис для изолированного учета трафика **белых списков** (выбранных нод) для пользователей Remnawave.

- Считает трафик только с нод, указанных в `WHITELISTS_NODE_IDS`;
- Расчет производится **on-demand** напрямую из таблицы `nodes_user_usage_history` базы данных Remnawave (без использования Redis);
- Автоматически исключает пользователя из внутреннего squad белых списков при исчерпании лимита и возвращает при пополнении/сбросе;
- Опционально принудительно обрывает активные сессии (`DROP_CONNECTIONS_ON_LIMIT=true`) и отправляет подписанный Webhook.

---

## Быстрый выбор варианта установки

| Вариант | Где работает сервис | База данных сервиса | Сеть |
| :--- | :--- | :--- | :--- |
| **[Вариант 1](#вариант-1-установка-на-одном-сервере-с-remnawave)** | На том же сервере, где стоит панель | В существующем PostgreSQL контейнере Remnawave | Общая Docker-сеть `remnawave-network` |
| **[Вариант 2](#вариант-2-установка-на-отдельном-сервере)** | На отдельном VPS | Автоматический локальный PostgreSQL в Docker Compose | Подключение к Remnawave по внешнему IP / домену |

---

## Вариант 1. Установка на одном сервере с Remnawave

Сервис запускается рядом с Remnawave и подключается к его контейнерам через внутреннюю Docker-сеть `remnawave-network`.

### Шаг 1. Создать базу и пользователя в PostgreSQL

Подключитесь к существующему контейнеру PostgreSQL панели (по умолчанию `remnawave-db`):

```bash
docker exec -it remnawave-db psql -U postgres
```

Выполните SQL-команды:

```sql
-- 1. Создаем отдельную базу данных для сервиса белых списков
CREATE USER whitelists WITH PASSWORD 'STRONG_DB_PASSWORD';
CREATE DATABASE whitelists OWNER whitelists;
GRANT ALL PRIVILEGES ON DATABASE whitelists TO whitelists;

-- Переключаемся на базу whitelists и выдаем права на схему
\c whitelists
GRANT ALL ON SCHEMA public TO whitelists;
ALTER SCHEMA public OWNER TO whitelists;

-- 2. Создаем read-only пользователя для чтения истории Remnawave
CREATE USER whitelists_reader WITH PASSWORD 'STRONG_READER_PASSWORD';
GRANT CONNECT ON DATABASE remnawave TO whitelists_reader;

\c remnawave
GRANT USAGE ON SCHEMA public TO whitelists_reader;
GRANT SELECT ON TABLE users, nodes_user_usage_history TO whitelists_reader;

\q
```

### Шаг 2. Настроить `.env`

В директории проекта создайте `.env`:

```bash
cp .env.example .env
nano .env
```

Заполните конфигурацию для одного сервера:

```env
# HTTP API
HTTP_ADDR=:8080
API_KEY=СГЕНЕРИРУЙТЕ_СЕКРЕТНЫЙ_КЛЮЧ_ОТ_32_СИМВОЛОВ

# База данных сервиса (в том же контейнере remnawave-db)
DATABASE_URL=postgresql://whitelists:STRONG_DB_PASSWORD@remnawave-db:5432/whitelists
DB_MAX_CONNS=4

# Доступ к базе Remnawave на чтение
REMNAWAVE_DATABASE_URL=postgresql://whitelists_reader:STRONG_READER_PASSWORD@remnawave-db:5432/remnawave
REMNAWAVE_DB_MAX_CONNS=4

# ID нод Remnawave, которые считаются белыми списками (через запятую)
WHITELISTS_NODE_IDS=1,2

# Подключение к API Remnawave внутри Docker-сети
REMNAWAVE_URL=http://remnawave:3000
REMNAWAVE_TOKEN=ВАШ_REMNAWAVE_API_TOKEN

# Управление доступом в squad белых списков
ENFORCE_WHITELIST_ACCESS=true
WHITELIST_SQUAD_ID=UUID_ВАШЕГО_WHITELIST_SQUAD
DROP_CONNECTIONS_ON_LIMIT=true

# Дополнительно (по умолчанию 0s для немедленного пересчета)
RECONCILE_SAFETY_MARGIN=0s
```

*Генерация надежного API_KEY (в терминале):*
```bash
openssl rand -hex 32
```

### Шаг 3. Запуск

```bash
docker compose up -d --build
```

Проверить статус:
```bash
docker compose logs -f
```

---

## Вариант 2. Установка на отдельном сервере

Сервис разворачивается на отдельном VPS. В этом случае поднимается собственный легковесный контейнер PostgreSQL для данных сервиса, а к БД и API Remnawave сервис обращается по сети.

```text
[ Сервер Remnawave ]                         [ Отдельный сервер Whitelists ]
- Remnawave API (порт 443/3000)      <───     - Whitelists Service (:8080)
- PostgreSQL Remnawave (порт 5432)   <───     - PostgreSQL whitelists-db (локальный)
```

### Шаг 1. Подготовка на сервере с Remnawave

1. **Создайте read-only пользователя в PostgreSQL панели:**

```bash
docker exec -it remnawave-db psql -U postgres
```

```sql
CREATE USER whitelists_reader WITH PASSWORD 'STRONG_READER_PASSWORD';
GRANT CONNECT ON DATABASE remnawave TO whitelists_reader;

\c remnawave
GRANT USAGE ON SCHEMA public TO whitelists_reader;
GRANT SELECT ON TABLE users, nodes_user_usage_history TO whitelists_reader;
\q
```

2. **Откройте порт PostgreSQL для удаленного подключения:**

В `docker-compose.yml` панели Remnawave для сервиса базы данных (`remnawave-db`) добавьте проброс порта:

```yaml
services:
  remnawave-db:
    # ...
    ports:
      - "5432:5432"
```

Перезапустите контейнер базы:
```bash
docker compose up -d remnawave-db
```

3. **Защита порта фаерволом (ОБЯЗАТЕЛЬНО):**

Не оставляйте порт 5432 открытым всему интернету. Разрешите доступ **только с IP-адреса отдельного сервера Whitelists**:

```bash
# Пример для UFW:
sudo ufw allow from IP_СЕРВЕРА_WHITELISTS to any port 5432 proto tcp
```

*(Либо настройте подключение через внутреннюю сеть WireGuard / Tailscale).*

---

### Шаг 2. Настройка на отдельном сервере Whitelists

1. Склонируйте репозиторий на отдельный сервер:
```bash
git clone https://github.com/mollyydev/remnawave-traffic-control.git /opt/whitelists-service
cd /opt/whitelists-service
```

2. Создайте файл `.env`:
```bash
cp .env.example .env
nano .env
```

Заполните настройки:

```env
# HTTP API
HTTP_ADDR=:8080
API_KEY=СГЕНЕРИРУЙТЕ_СЕКРЕТНЫЙ_КЛЮЧ_ОТ_32_СИМВОЛОВ

# Локальная база данных (поднимается автоматически в docker-compose.standalone.yml)
DB_USER=whitelists
DB_PASSWORD=ПРИДУМАЙТЕ_ПАРОЛЬ_ДЛЯ_ЛОКАЛЬНОЙ_БД
DB_NAME=whitelists
DATABASE_URL=postgresql://whitelists:ПРИДУМАЙТЕ_ПАРОЛЬ_ДЛЯ_ЛОКАЛЬНОЙ_БД@whitelists-db:5432/whitelists
DB_MAX_CONNS=4

# Удаленное подключение к БД Remnawave (IP или домен сервера Remnawave)
REMNAWAVE_DATABASE_URL=postgresql://whitelists_reader:STRONG_READER_PASSWORD@IP_СЕРВЕРА_REMNAWAVE:5432/remnawave
REMNAWAVE_DB_MAX_CONNS=4

# ID нод белых списков в панели Remnawave
WHITELISTS_NODE_IDS=1,2

# Публичный URL панели Remnawave и токен
REMNAWAVE_URL=https://panel.yourdomain.com
REMNAWAVE_TOKEN=ВАШ_REMNAWAVE_API_TOKEN

# Управление squad белых списков
ENFORCE_WHITELIST_ACCESS=true
WHITELIST_SQUAD_ID=UUID_ВАШЕГО_WHITELIST_SQUAD
DROP_CONNECTIONS_ON_LIMIT=true

RECONCILE_SAFETY_MARGIN=0s
```

### Шаг 3. Запуск через standalone compose

На отдельном сервере запуск выполняется через готовый файл `docker-compose.standalone.yml`:

```bash
docker compose -f docker-compose.standalone.yml up -d --build
```

Этот файл автоматически запустит локальный контейнер PostgreSQL (`whitelists-db`) и сам сервис.

Проверить логи:
```bash
docker compose -f docker-compose.standalone.yml logs -f
```

---

## Проверка работоспособности

Выполните запрос к эндпоинту `/healthz`:

```bash
curl -H "X-API-Key: ВАШ_API_KEY" http://127.0.0.1:8080/healthz
```

Ожидаемый ответ:
```text
HTTP 200 OK
```

---

## API Справочник

Все запросы требуют обязательный заголовок аутентификации:
```http
X-API-Key: ВАШ_API_KEY
```

### 1. Получить трафик пользователя

```http
GET /v1/users/{id}/trafic
```

При этом запросе сервис **on-demand** пересчитывает актуальный использованный трафик из базы Remnawave за текущий расчетный период пользователя.

**Пример ответа (200 OK):**
```json
{
  "total": 107374182400,
  "used": 34359738368
}
```
*(Все значения возвращаются в байтах. `total: 0` означает безлимитный доступ).*

---

### 2. Подключить пользователя к учету белых списков

```http
POST /v1/users/{id}
Content-Type: application/json
```

**Тело запроса:**
```json
{
  "total": 50,
  "remnawave_id": 123,
  "limit_reset": 30
}
```

- `total` *(float, обязательно)* — объем квоты в **гигабайтах** (GB). `0` — безлимит.
- `remnawave_id` *(int, обязательно)* — числовой ID пользователя в панели Remnawave.
- `limit_reset` *(int, опционально)*:
  - Если **не указан**: период сброса синхронизируется с панелью Remnawave (день, неделя, месяц, 30-дневное скользящее окно);
  - Если **число** (например, `30`): персональный цикл сброса каждые N дней от момента создания.

---

### 3. Изменить квоту или период сброса

```http
PUT /v1/users/{id}
Content-Type: application/json
```

**Тело запроса:**
```json
{
  "total": 100,
  "limit_reset": null
}
```

- `total` *(опционально)* — новый общий лимит в GB.
- `limit_reset`:
  - `30` — переключить на цикл сброса каждые 30 дней;
  - `null` — вернуть автоматическую синхронизацию сброса с панелью Remnawave;
  - если поле не передано — текущий режим сброса остается без изменений.

---

### 4. Ручной сброс использованного трафика

```http
POST /v1/users/{id}/trafic/reset
```

Сбрасывает счетчик использованного трафика в `0`, открывает новый период учета и восстанавливает squad белых списков в Remnawave.

---

### 5. Отключить пользователя от учета белых списков

```http
DELETE /v1/users/{id}
```

Удаляет пользователя из базы учета.

---

## Описание переменных окружения

| Переменная | Обязательная | Описание |
| :--- | :---: | :--- |
| `HTTP_ADDR` | Нет | Адрес и порт HTTP сервера (по умолчанию `:8080`) |
| `API_KEY` | **Да** | Секретный ключ для доступа к API сервиса (минимум 32 символа) |
| `DATABASE_URL` | **Да** | DSN подключения к собственной PostgreSQL базе данных сервиса `whitelists` |
| `REMNAWAVE_DATABASE_URL` | **Да** | DSN подключения к базе данных Remnawave с правами SELECT на `nodes_user_usage_history` и `users` |
| `WHITELISTS_NODE_IDS` | **Да** | Список ID нод Remnawave через запятую (например `1,2,5`), трафик с которых считается белыми списками |
| `REMNAWAVE_URL` | **Да** | URL панели Remnawave (`http://remnawave:3000` или `https://panel.domain.com`) |
| `REMNAWAVE_TOKEN` | **Да** | API токен Remnawave с правами на управление пользователями и squad |
| `ENFORCE_WHITELIST_ACCESS`| Нет | `true` — автоматически удалять squad при перелимите и возвращать при наличии квоты |
| `WHITELIST_SQUAD_ID` | При Enforce | UUID внутреннего squad белых списков в Remnawave |
| `DROP_CONNECTIONS_ON_LIMIT` | Нет | `true` — принудительно обрывать текущие соединения при исчерпании лимита |
| `RECONCILE_SAFETY_MARGIN` | Нет | Защитный интервал отсечки истории (рекомендуется `0s` для моментального on-demand расчета) |
| `WEBHOOK_ENABLED` | Нет | `true` — отправлять подписанный HTTP вебхук при исчерпании лимита |
| `WEBHOOK_URL` | При Webhook | URL приемника вебхуков |
| `WEBHOOK_SECRET` | При Webhook | Секретный ключ для HMAC-SHA256 подписи вебхука |
