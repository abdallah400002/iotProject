import sys
sys.path.insert(0, '/app')

import os
import json
import time
from datetime import datetime

from kafka import KafkaConsumer
from sqlalchemy.orm import Session

from app.database import SessionLocal, engine, Base
from app import models

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "stock_prices"

Base.metadata.create_all(bind=engine)


def process_message(db: Session, redis_client, msg: dict):
    symbol = msg["symbol"]
    price = float(msg["price"])
    ts = datetime.utcfromtimestamp(int(msg["timestamp"]))

    stock = db.query(models.Stock).filter(models.Stock.symbol == symbol).first()
    if not stock:
        print(f"Unknown symbol {symbol}, skipping")
        return

    record = models.PriceHistory(stock_id=stock.id, price=price, timestamp=ts)
    db.add(record)
    db.commit()

    key = f"latest_price:{symbol}"
    redis_client.set(
        key,
        json.dumps({"symbol": symbol, "price": price, "timestamp": ts.isoformat()}),
    )

    print(f"Stored price for {symbol}: {price} @ {ts.isoformat()}")


def main():
    import redis
    redis_client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="price-consumer-group",
    )

    print(f"Consuming from {TOPIC} on {KAFKA_BOOTSTRAP_SERVERS} ...")

    db = SessionLocal()
    try:
        for message in consumer:
            try:
                process_message(db, redis_client, message.value)
            except Exception as e:
                print("Error processing message:", e)
                db.rollback()
    finally:
        db.close()
        consumer.close()


if __name__ == "__main__":
    time.sleep(5)
    main()