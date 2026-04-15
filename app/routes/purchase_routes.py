from typing import Optional, List
from fastapi import APIRouter, Query, UploadFile, File, Form, Depends
from datetime import datetime, date
#from app.core.security import verify_access_token
from app.services import purchase_service
from app.schemas.inventory_schemas import PurchaseUpdateModel

router = APIRouter(tags=["PURCHASE"])

@router.get("/getPurchaseSources_1_0", description="Get purchase sources for items")
def get_purchase_sources(country: Optional[str] = None, entityType: Optional[str] = None):
    return purchase_service.get_purchase_sources(country, entityType)

@router.post("/createPurchaseInvoice_1_0")
async def create_purchase_invoice(
        purchase: str = Form(...),
        invoiceFiles: List[UploadFile] = File(None),
        otherFiles: List[UploadFile] = File(None)
):
    return await purchase_service.create_purchase_invoice(purchase, invoiceFiles, otherFiles)

@router.get("/getPurchaseList_1_0", description="Get paginated list of purchases with status and counts")
def get_purchase_list(
        startDate: str = None, 
        endDate: str = None, 
        pageNo: int = Query(1, ge=1), 
        pageSize: int = Query(15, ge=1), 
        storeId: int = None
):
    return purchase_service.get_purchase_list(startDate, endDate, pageNo, pageSize, storeId)

@router.get("/getPurchaseDetails_1_0", description="Get purchase details with items and files")
def get_purchase_details(purchaseId: int):
    return purchase_service.get_purchase_details(purchaseId)

@router.put("/updatePurchase_1_0")
def update_purchase(data: PurchaseUpdateModel):
    return purchase_service.update_purchase(data)