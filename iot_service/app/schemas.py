from datetime import datetime
from pydantic import BaseModel


class StockCreate(BaseModel):
    symbol: str
    company_name: str


class StockOut(BaseModel):
    id: int
    symbol: str
    company_name: str

    class Config:
        orm_mode = True


class PriceOut(BaseModel):
    symbol: str
    price: float
    timestamp: datetime


class PriceHistoryOut(BaseModel):
    price: float
    timestamp: datetime

    class Config:
        orm_mode = True