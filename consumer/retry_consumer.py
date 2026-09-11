import time

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
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
GROUP_ID = "order-aggregator-retry"

MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 1.0

SIMULATE_FAILURES = True
SIMULATE_EVERY_N_ORDERS = 3
SIMULATE_FAIL_ATTEMPTS = 2

TEMPORARY_ERRORS = (ConnectionError, TimeoutError, OSError)


def is_temporary_error(error: Exception) -> bool:
    return isinstance(error, TEMPORARY_ERRORS)


def maybe_simulate_failure(attempt: int, simulate: bool) -> None:
    if simulate and attempt <= SIMULATE_FAIL_ATTEMPTS:
        raise ConnectionError(f"Simulated temporary failure (attempt {attempt})")


def process_order(order: dict, count: int, total_price: float) -> tuple[int, float, float]:
    count += 1
    total_price += order["price"]
    running_avg = total_price / count
    return count, total_price, running_avg


def process_with_retry(
    deserializer: AvroDeserializer,
    context: SerializationContext,
    msg,
    count: int,
    total_price: float,
    simulate_failure: bool = False,
) -> tuple[dict, int, float, float]:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            maybe_simulate_failure(attempt, simulate_failure)
            order = deserializer(msg.value(), context)
            count, total_price, running_avg = process_order(order, count, total_price)
            return order, count, total_price, running_avg
        except Exception as error:
            last_error = error
            if not is_temporary_error(error) or attempt == MAX_RETRIES:
                raise

            wait = INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(
                f"Temporary failure on attempt {attempt}/{MAX_RETRIES}: {error}. "
                f"Retrying in {wait:.1f}s..."
            )
            time.sleep(wait)

    raise last_error


def main():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(schema_registry, ORDER_SCHEMA)
    context = SerializationContext(TOPIC, MessageField.VALUE)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    count = 0
    total_price = 0.0
    orders_seen = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            orders_seen += 1
            simulate_failure = (
                SIMULATE_FAILURES and orders_seen % SIMULATE_EVERY_N_ORDERS == 0
            )

            try:
                order, count, total_price, running_avg = process_with_retry(
                    deserializer,
                    context,
                    msg,
                    count,
                    total_price,
                    simulate_failure=simulate_failure,
                )
                print(
                    f"Order {order['orderId']}: {order['product']} @ {order['price']:.2f} "
                    f"| running avg: {running_avg:.2f} ({count} orders)"
                )
                consumer.commit(message=msg)
            except Exception as error:
                print(f"Failed after retries: {error}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
