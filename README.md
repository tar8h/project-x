# Платформа прогнозируемого обслуживания оборудования

Комплексная система потоковой обработки данных и машинного обучения для обнаружения аномалий и предсказания отказов оборудования на основе телеметрии IoT устройств.
Платформа моделирует поток сенсорных данных, обрабатывает их через распределенный стриминговый пайплайн и применяет ML модели для выявления неисправностей.

## Используемый стек

- Python
- Docker Compose
- Apache Kafka
- Apache Flink
- Apache Spark
- TimescaleDB
- Grafana
- MLflow
- FastApi
- MQTT (Mosquitto)
- MinIO
- Dagster

## Архитектура системы

Поток данных в системе:

MQTT -> Telegraf -> Kafka -> MinIO -> PyFlink -> TimescaleDB -> Grafana

* MQTT (Mosquitto) эмитирует работу датчиков, отправляющих телеметрию.
* Telegraf собирает данные из MQTT и отправляет их в Kafka.
* Kafka центральный брокер событий.
* MinIO хранение сырых данных.
* PyFlink потоковая обработка данных (feature engineering, расчет аномалий, генерация предсказаний).
* TimescaleDB хранение временных рядов и результатов обработки.
* Grafana визуализация метрик и мониторинг системы.

## Машинное обучение

В системе используется два подхода:

Isolation Forest (обучение без учителя)
* Используется для обнаружения аномалий
* Сохраняется как (models/isolation_forest.joblib)

Random Forest (обучение с учителем)
* Обучается на размеченных данных (failure)
* Используется для предсказания отказов оборудования
* Регистрируется в MLflow Model Registry

## Оценка моделей

Модели сравниваются по метрикам:

* Precision (точность обнаружения отказов)
* Recall (полнота обнаружения отказов)
* F1-score (баланс между ложными срабатываниями и пропущенными отказами)

## Демонстрационный сценарий

Требования

* Docker + Docker Compose
* Python 3.10+
* Java (Flink jobs)

1. Скопируйте репозиторий

```
git clone <https://github.com/tar8h/project-x.git>
cd project
```

2. Скопируйте переменные окружения

```
Copy-Item .env.example .env
```

3. Создание виртуального окружения:

```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r services/ml/requirements.txt
```

4. Запуск инфраструктуры

```
docker compose up -d
```

5. Генерация тестовых данных

```
docker compose --profile load up -d generator
```

6. Обучение модели Isolation Forest

```
python services/ml/train_isolation_forest.py --output models/isolation_forest.joblib
```

7. Демонстрационный запуск

```
docker compose --profile ui --profile processing --profile orchestration --profile load up -d
```

## Проверка пайплайна

Проверить:

* Kafka UI -> поток событий
* MinIO -> сырые данные (sensor_raw)
* TimescaleDB -> временные ряды
* Grafana -> дашборды

Основные локальные интерфейсы:

- FastAPI:       <http://localhost:8080/docs>
- MLflow:        <http://localhost:5000>
- MinIO Console: <http://localhost:9001> (minioadmin/minioadmin)
- Grafana:       <http://localhost:3000> (admin/admin)
- Kafka UI:      <http://localhost:8088>
- Flink UI:      <http://localhost:8081>
- Spark Master:  <http://localhost:8082>
- Spark Worker:  <http://localhost:8083>
- Dagster:       <http://localhost:3001>
- pgAdmin        <http://localhost:5050>

Логин:

```
admin@example.com / admin
```

Подключение к TimescaleDB из pgAdmin:

```
Host: timescaledb
Port: 5432
Database: maintenance
User: maintenance
Password: maintenance
```

## Ограничения MVP

* Разметка отказов синтетическая и используется только для тестирования
* Isolation Forest выдает score, а не истинную вероятность отказа
* Random Forest требует исторических данных об отказах и ремонтах
* Система предназначена для демонстрации архитектуры

## Структура проекта

```
infra/
  grafana/               # панель мониторинга
  mosquitto/             # MQTT конфигурация брокера
  telegraf/              # Мост MQTT -> Kafka edge/ingestion 
  timescaledb/           # схема и гипертаблицы
jobs/
  dagster/               # оркестрация
  flink/                 # потоковая обработка
  spark/                 # пакетная обработка
services/
  api/                   # FastAPI inference
  generator/             # генератор синтетических данных
  ingestion/             # Kafka -> MinIO сырые данные
  ml/                    # скрипты обучения
models/                  # локально обученные модели, игнорируется в git по умолчанию
```

## Лицензия

MIT
