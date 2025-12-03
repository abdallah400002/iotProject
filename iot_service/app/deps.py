import os
import redis 
from fastapi import HTTPException
from sqlalchemy.orm import Session

from . import models

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

def get_redis():
    return redis.from_url(REDIS_URL, decode_responses=True)

def get_stock_or_404(db: Session, symbol: str) -> models.Stock:
    stock = db.query(models.Stock).filter(models.Stock.symbol == symbol).first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    return stock