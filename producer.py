import psycopg2
from kafka import KafkaProducer
import json
import time

# Инициализация подключений
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
conn.autocommit = False

# Добавляем колонку если её нет
cursor = conn.cursor()
try:
    cursor.execute("""
        ALTER TABLE user_logins 
        ADD COLUMN IF NOT EXISTS sent_to_kafka BOOLEAN DEFAULT FALSE
    """)
    conn.commit()
    print("Column 'sent_to_kafka' added or already exists")
except Exception as e:
    print(f"Error adding column: {str(e)}")
    conn.rollback()
finally:
    cursor.close()


def process_batch(batch_size=100):
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT id, username, event_type, extract(epoch FROM event_time)
            FROM user_logins
            WHERE sent_to_kafka = FALSE
            ORDER BY event_time
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        """, (batch_size,))

        rows = cursor.fetchall()

        for row in rows:
            record_id, username, event_type, timestamp = row
            data = {
                "id": record_id,
                "user": username,
                "event": event_type,
                "timestamp": float(timestamp)
            }

            try:
                future = producer.send("user_events", value=data)
                future.get(timeout=10)

                cursor.execute("""
                    UPDATE user_logins
                    SET sent_to_kafka = TRUE
                    WHERE id = %s
                """, (record_id,))

                print(f"Sent and marked: {record_id}")

            except Exception as e:
                print(f"Error processing record {record_id}: {str(e)}")
                conn.rollback()
                return False

        conn.commit()
        return True

    finally:
        cursor.close()


if __name__ == "__main__":
    while True:
        success = process_batch()
        if not success or not conn.status:
            conn = psycopg2.connect(
                dbname="test_db", user="admin", password="admin",
                host="localhost", port=5432
            )

        time.sleep(1)