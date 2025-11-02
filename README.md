# Проект L0 — демо микросервис обработки заказов

## Описание

**L0** — демонстрационный микросервис на Go, реализующий обработку заказов с использованием:

* брокера сообщений **Kafka**,
* базы данных **PostgreSQL**,
* локального кеша для ускоренного доступа,
* и простого **веб-интерфейса** для просмотра заказов по ID.

Архитектура проекта построена по принципам микросервисов и разделения ответственности:
## Архитектура

* **Producer (cmd/produser)**
  → генерирует случайный заказ (`gofakeit`), сериализует в JSON и отправляет в Kafka.

* **Consumer (cmd/app)**
  → слушает Kafka-топик, валидирует JSON, парсит `Order`, сохраняет в PostgreSQL, кэширует.

* **Repository**
  → извлекает заказы из БД при отсутствии в кеше, возвращает `model.Order`.

* **HTTP API (internal/httpapi)**
  → `/form` — ввод ID заказа
  → `/order?id=...` — возвращает JSON заказа.

---

## Структура проекта

```
L0/
├── README.md
├── cmd/
│   ├── app/          # Консьюмер — принимает сообщения из Kafka, сохраняет в БД, отдаёт через HTTP API
│   │   └── main.go
│   └── produser/     # Продюсер — генерирует тестовые заказы и отправляет их в Kafka
│       └── main.go
├── docker-compose.yaml
├── go.mod
├── go.sum
├── image.png
└── internal/
    ├── config/       # Загрузка конфигурации (Kafka, PostgreSQL, HTTP)
    │   └── config.go
    ├── httpapi/      # HTTP-сервер и HTML-интерфейс
    │   ├── form.html
    │   └── handler.go
    ├── model/        # Модели данных и валидация заказов
    │   ├── model.go
    │   └── validate.go
    ├── repository/   # Репозиторий для работы с БД и кешем
    │   └── repository.go
    └── util/         # Утилиты (измерение времени выполнения)
        └── duration.go

```

---

## Технологии

| Компонент        | Используется                                 |
| ---------------- | -------------------------------------------- |
| Язык             | Go 1.22+                                     |
| БД               | PostgreSQL 16                                |
| Брокер           | Kafka 7.5.0 (Confluent Platform)             |
| Кеш              | Встроенная `map[string]Order` с блокировками |
| Генератор данных | `github.com/brianvoe/gofakeit/v7`            |
| Коннектор к БД   | `github.com/jackc/pgx/v5/pgxpool`            |
| Kafka-клиент     | `github.com/segmentio/kafka-go`              |
| Контейнеризация  | Docker + Docker Compose                      |

---

## Запуск

### 1. Поднять инфраструктуру

Из папки `L0/`:

```bash
docker compose up -d zookeeper-sandbox kafka-broker-sandbox postgres
```

Проверить, что всё работает:

```bash
docker ps
```

Kafka доступна:

* для контейнеров — `kafka-broker-sandbox:29092`
* для хоста — `localhost:9093`

Postgres доступен:

* пользователь: `l0`
* пароль: `L0`
* база: `l0_wb`
* порт: `5432`

---

## Настройка базы данных PostgreSQL

Перед запуском приложения необходимо развернуть базу данных **PostgreSQL**
с указанной конфигурацией и структурой таблиц.

### 🔧 Конфигурация БД

| Параметр                       | Значение                                                                              |
| ------------------------------ | ------------------------------------------------------------------------------------- |
| Имя базы данных                | `l0_wb`                                                                               |
| Пользователь                   | `l0`                                                                                  |
| Пароль                         | `L0`                                                                                  |
| Порт (для подключения с хоста) | `5433`                                                                                |
| Порт внутри контейнера         | `5432`                                                                                |
| Хост                           | `localhost` (для приложений с хоста) / `postgres` (для приложений внутри Docker-сети) |

**Пример подключения (DSN):**

```
postgres://l0:L0@localhost:5433/l0_wb?sslmode=disable
```

**Docker Compose-сервис PostgreSQL:**

```yaml
postgres:
  image: postgres:16
  container_name: l0-postgres
  environment:
    POSTGRES_DB: l0_wb
    POSTGRES_USER: l0
    POSTGRES_PASSWORD: L0
  ports:
    - "5433:5432"
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U l0 -d l0_wb"]
    interval: 5s
    timeout: 3s
    retries: 20
```

---

### Структура таблиц

После запуска контейнера нужно создать таблицы для хранения данных заказов.
Для этого можно подключиться к БД и выполнить SQL-скрипт:

```bash
psql "postgres://l0:L0@localhost:5433/l0_wb?sslmode=disable" -v ON_ERROR_STOP=1 <<'SQL'
-- ====== ORDERS ======
CREATE TABLE IF NOT EXISTS public.orders (
  order_uid          TEXT PRIMARY KEY,
  track_number       TEXT        NOT NULL,
  entry              TEXT        NOT NULL,
  locale             TEXT,
  internal_signature TEXT,
  customer_id        TEXT        NOT NULL,
  delivery_service   TEXT,
  shardkey           TEXT,
  sm_id              INT,
  date_created       TIMESTAMPTZ NOT NULL,
  oof_shard          TEXT
);

-- ====== DELIVERIES ======
-- В JSON это тоже order_uid, в БД храним как order_id (FK на orders.order_uid)
CREATE TABLE IF NOT EXISTS public.deliveries (
  order_id TEXT PRIMARY KEY
           REFERENCES public.orders(order_uid) ON DELETE CASCADE,
  name     TEXT,
  phone    TEXT,
  zip      TEXT,
  city     TEXT,
  address  TEXT,
  region   TEXT,
  email    TEXT
);

-- ====== PAYMENTS ======
CREATE TABLE IF NOT EXISTS public.payments (
  order_id      TEXT PRIMARY KEY
                REFERENCES public.orders(order_uid) ON DELETE CASCADE,
  transaction   TEXT    UNIQUE NOT NULL,
  request_id    TEXT,
  currency      TEXT    NOT NULL,
  provider      TEXT,
  amount        DOUBLE PRECISION NOT NULL,  -- float64
  payment_dt    BIGINT NOT NULL,            -- int64 (unix)
  bank          TEXT,
  delivery_cost INT,
  goods_total   INT,
  custom_fee    INT
);

-- ====== ORDER ITEMS ======
CREATE TABLE IF NOT EXISTS public.order_items (
  id           BIGSERIAL PRIMARY KEY,
  order_id     TEXT REFERENCES public.orders(order_uid) ON DELETE CASCADE,
  chrt_id      BIGINT,       -- int64
  track_number TEXT,
  price        INT,
  rid          TEXT,
  name         TEXT,
  sale         INT,
  size         TEXT,
  total_price  INT,
  nm_id        BIGINT,       -- int64
  brand        TEXT,
  status       INT
);

-- Полезные индексы
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON public.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_transaction  ON public.payments(transaction);

SQL
```

Проверить, что таблицы созданы:

```bash
psql "postgres://l0:L0@localhost:5432/l0_wb?sslmode=disable" -c "\dt"
```

---

### 2. Запустить **consumer**

```bash
go run cmd/app/main.go
```
После запуска появятся строки:
```bash
<гггг/мм/дд чч:мм:сс> Консьюмер подписан на топик 'my-learning-topic' в группе 'my-learning-go-group'
<гггг/мм/дд чч:мм:сс> Пул соединений успешно настроен
<гггг/мм/дд чч:мм:сс> начинаю слушать localhost::8081
```
HTTP-сервер поднимется на `http://localhost:8081/form`.

---

### 3. Запустить **producer**

В отдельном окне:

```bash
go run cmd/produser/main.go
```

Producer начнёт генерировать случайные заказы и отправлять их в Kafka каждые 0.5 секунды.

если в окне с producer будут появляться строчки по типу 
```bash
2025/11/02 08:54:56 json c id= 725f7847-9034-4f85-9ee1-0e3dc4c64fd5 сгенерирван успешно
2025/11/02 08:54:56 Ошибка отправки сообщения в Кафку id: '725f7847-9034-4f85-9ee1-0e3dc4c64fd5': dial tcp 127.0.0.1:9093: connect: connection refused
```
а в consumer таки строчки:
```bash
2025/11/02 08:54:51 ошибка FetchMessage из Kafka: failed to dial: failed to open connection to localhost:9093: dial tcp 127.0.0.1:9093: connect: connection refused
```

но при этом вы уверены, что докер поднят, то попробуйте перезапустить его:
```bash
docker compose down
docker compose up -d zookeeper-sandbox kafka-broker-sandbox postgres
```
---

### 5. Проверка работы

1. Открой `http://localhost:8081/form`
2. Введи `order_id`, который был в логах producer (`json c id= ...`)
3. Нажми **Показать заказ** — появится страница с деталями заказа.

---

## Корректное завершение работы
Чтобы завершить сервис, в окне с консьюмером нажмите Ctrl + C
После этого должны появиться строки:
```bash
^Cзакрываю канал
отменяю контекст
закрываю горутины
стопаю сигн
```
---

