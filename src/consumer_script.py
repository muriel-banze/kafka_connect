import json
import psycopg2
from kafka import KafkaConsumer


# Kafka Configuration
#KAFKA_BROKER = "localhost:9092"
#TOPIC = "oracle-cdc.INVENTORY.CUSTOMERS"

# PostgreSQL Connection
conn = psycopg2.connect(
    dbname="customer_orders",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Kafka Consumer Setup
consumer = KafkaConsumer(
    "oracle-cdc.INVENTORY.CUSTOMERS",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest"
)

print("Listening for CDC events...")

for message in consumer:
    try:
        event = message.value
        payload = event.get("payload", {})

        before = payload.get("before", {})
        after = payload.get("after", {})
        op = payload.get("op", "")  # "c" = insert, "u" = update, "d" = delete

        table_name = "customers"  # Target PostgreSQL table

        if op in ["c", "u"]:  # Insert or Update
            columns = ", ".join(f"\"{col}\"" for col in after.keys())
            values_placeholder = ", ".join(["%s"] * len(after))
            values = tuple(after.values())

            upsert_query = f"""
                INSERT INTO public."{table_name}" ({columns})
                VALUES ({values_placeholder})
                ON CONFLICT (id) DO UPDATE SET
                {", ".join(f'"{col}" = EXCLUDED."{col}"' for col in after.keys())};
            """

            cursor.execute(upsert_query, values)
            conn.commit()
            print(f"Upserted: {after}")

        elif op == "d":  # Delete operation
            delete_query = f'DELETE FROM public."{table_name}" WHERE id = %s;'
            cursor.execute(delete_query, (before["ID"],))
            conn.commit()
            print(f"Deleted: {before}")

    except Exception as e:
        print(f"Error processing message: {e}")

# Close connections (never reached in an infinite loop)
cursor.close()
conn.close()
