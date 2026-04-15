from typing import Optional
from fastapi import APIRouter, Query, Depends
from datetime import date
#from app.core.security import verify_access_token
from app.services import return_service
from app.schemas.inventory_schemas import CreateReturnModel, UpdateReturnModel

router = APIRouter(tags=["RETURN"])

@router.get("/getReturnableStock_1_0", description="get returnable stock")
def get_returnable_stock(returnFromId: int):
    return return_service.get_returnable_stock(returnFromId)

@router.post("/addReturn_1_0", description="Add return data")
def add_return(data: CreateReturnModel):
    return return_service.add_return(data)

@router.get("/getReturnList_1_0", description="Get Return List")
def get_return_list(
    startDate: date = None,
    endDate: date = None,
    pageNo: int = Query(1, ge=1),
    pageSize: int = Query(15, ge=1),
    search: str = None,
    status: str = None,
    storeId: int = None
):
    return return_service.get_return_list(startDate, endDate, pageNo, pageSize, search, status, storeId)

@router.get("/getReturnDetails_1_0", description="get return more details")
def get_return_details(returnId: int):
    return return_service.get_return_details(returnId)

@router.put("/updateReturn_1_0", description="Update return details")
def update_return(returnId: int, data: UpdateReturnModel):
    return return_service.update_return(returnId, data)