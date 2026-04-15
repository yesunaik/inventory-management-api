from typing import Optional
from fastapi import APIRouter, Query, Depends
from datetime import datetime
#from app.core.security import verify_access_token
from app.services import stock_service

router = APIRouter(tags=["STOCK"])

@router.get("/getStockSummary_1_0", description="Get stock summary")
def get_stock_summary(
    page: int = 1,
    pageSize: int = 15,
    search: str = None,
    startDate: datetime = None,
    endDate: datetime = None,
    storeId: int = None
):
    return stock_service.get_stock_summary(page, pageSize, search, startDate, endDate, storeId)

@router.get("/getClosingStatement_1_0", description="Get closing statement for item")
def get_closing_statement(
    itemId: int,
    startDate: datetime,
    endDate: datetime,
    storeId: int = None
):
    return stock_service.get_closing_statement(itemId, startDate, endDate, storeId)