from kafka import KafkaConsumer
import clickhouse_connect
import json
from datetime import datetime

# Инициализация потребителя
consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers="localhost:9092",
    group_id="clickhouse_loader_group",  # Важно для управления offset'ами
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Подключение к ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='user',
    password='strongpassword'
)

# Создание таблицы (если не существует)
client.command("""
CREATE TABLE IF NOT EXISTS user_logins (
    id UInt32,
    username String,
    event_type String,
    event_time DateTime,
    kafka_processed_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (event_time, id)
""")

# Обработка сообщений
for message in consumer:
    try:
        data = message.value
        print(f"Processing record ID: {data['id']}")

        # Безопасная вставка с параметризованным запросом
        client.insert(
            "user_logins",
            [[
                data['id'],
                data['user'],
                data['event'],
                datetime.fromtimestamp(data['timestamp'])
            ]],
            column_names=['id', 'username', 'event_type', 'event_time']
        )

    except Exception as e:
        print(f"Error processing message: {str(e)}")