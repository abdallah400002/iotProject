from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime
import json

from .database import Base, engine, get_db
from . import models, schemas
from .deps import get_redis, get_stock_or_404

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Real-Time Stock Price Service")


@app.post("/stocks", response_model=schemas.StockOut)
def add_stock(stock_in: schemas.StockCreate, db: Session = Depends(get_db)):
    existing = db.query(models.Stock).filter(models.Stock.symbol == stock_in.symbol).first()
    if existing:
        raise HTTPException(status_code=400, detail="Symbol already exists")

    stock = models.Stock(symbol=stock_in.symbol, company_name=stock_in.company_name)
    db.add(stock)
    db.commit()
    db.refresh(stock)
    return stock


@app.get("/stocks/{symbol}/price", response_model=schemas.PriceOut)
def get_latest_price(
    symbol: str,
    db: Session = Depends(get_db),
    redis_client=Depends(get_redis),
):
    stock = get_stock_or_404(db, symbol)

    key = f"latest_price:{stock.symbol}"
    data = redis_client.get(key)
    if not data:
        last = (
            db.query(models.PriceHistory)
            .filter(models.PriceHistory.stock_id == stock.id)
            .order_by(models.PriceHistory.timestamp.desc())
            .first()
        )
        if not last:
            raise HTTPException(status_code=404, detail="No price data yet")
        return {
            "symbol": stock.symbol,
            "price": last.price,
            "timestamp": last.timestamp,
        }

    obj = json.loads(data)
    return {
        "symbol": stock.symbol,
        "price": obj["price"],
        "timestamp": datetime.fromisoformat(obj["timestamp"]),
    }


@app.get("/stocks/{symbol}/history", response_model=List[schemas.PriceHistoryOut])
def get_price_history(
    symbol: str,
    limit: int = 10,
    db: Session = Depends(get_db),
):
    stock = get_stock_or_404(db, symbol)

    rows = (
        db.query(models.PriceHistory)
        .filter(models.PriceHistory.stock_id == stock.id)
        .order_by(models.PriceHistory.timestamp.desc())
        .limit(limit)
        .all()
    )
    return rows