# Инструкция по запуску

Требования: установлен Docker и Docker Compose.

## 1) Сборка и запуск

Из корня репозитория:

```bash
docker-compose up --build -d
```

Что произойдет:
1) Поднимутся `postgres`, `kafka`, `jobmanager`, `taskmanager`.
2) Контейнер `kafka-producer` прочитает все CSV из `исходные данные/`, преобразует строки в JSON и отправит в Kafka топик `sales_topic`.
3) Контейнер `flink-star-job`  запустит джобу `pyflink-job/sales_star_job.py`, прочитает Kafka и запишет данные в PostgreSQL.

## 2) Проверка

```bash
docker ps

# Логи
docker-compose logs -f kafka-producer
docker-compose logs -f flink-star-job

# Flink UI
# http://localhost:8082

# Проверка данных в Postgres (нужно подождать примерно минуту)
docker-compose exec postgres psql -U user -d snowflake_db -c "SELECT COUNT(*) FROM fact_sales;"

docker-compose exec postgres psql -U user -d snowflake_db -c "SELECT * FROM fact_sales LIMIT 10;"
```

## Порты

| Сервис | Порт на хосте | Описание |
|--------|---------------|----------|
| Kafka | 9093 | брокер (PLAINTEXT на `localhost:9093`) |
| PostgreSQL | 5443 | база `snowflake_db` (в контейнере 5432) |
| Flink UI | 8082 | web UI Flink |

## Остановка

```bash
docker-compose down
```
