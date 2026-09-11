import random
import time
import uuid

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

ORDER_SCHEMA = """{
  "type": "record",
  "name": "Order",
  "namespace": "com.example",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}"""

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"
PRODUCTS = ("Item1", "Item2", "Item3", "Item4", "Item5")


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def main():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    serializer = AvroSerializer(
        schema_registry,
        ORDER_SCHEMA,
        lambda order, _: order,
    )

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    context = SerializationContext(TOPIC, MessageField.VALUE)

    try:
        while True:
            order = {
                "orderId": str(uuid.uuid4()),
                "product": random.choice(PRODUCTS),
                "price": round(random.uniform(10.0, 500.0), 2),
            }
            producer.produce(
                TOPIC,
                value=serializer(order, context),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            print(f"Sent order: {order}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
