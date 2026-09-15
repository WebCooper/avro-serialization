for kafka
docker compose up -d

to start producing orders 
uv run producer.py

to start aggregation consumer
uv run consumer/consumer.py

to start retry logic consumer
uv run consumer/retry_consumer.py

to start dlq consumer
uv run consumer/dlq_consumer.py