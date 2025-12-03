import os
import json
import random
import time
import datetime as dt
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "stock_prices"

SYMBOLS = ["AAPL", "MSFT", "GOOG", "TSLA", "AMZN"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def generate_price(base: float) -> float:
    delta = random.uniform(-1.0, 1.0)
    return round(max(1.0, base + delta), 2)


def main():
    base_prices = {s: random.uniform(100, 300) for s in SYMBOLS}
    print(f"Producing to {TOPIC} on {KAFKA_BOOTSTRAP_SERVERS} ...")
    while True:
        for symbol in SYMBOLS:
            base_prices[symbol] = generate_price(base_prices[symbol])
            msg = {
                "symbol": symbol,
                "price": base_prices[symbol],
                "timestamp": int(dt.datetime.utcnow().timestamp()),
            }
            producer.send(TOPIC, msg)
            print("Sent:", msg)
        producer.flush()
        time.sleep(1)


if __name__ == "__main__":
    main()