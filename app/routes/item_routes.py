from typing import Optional, List
from fastapi import APIRouter, Query, UploadFile, File, Form, Depends
#from app.core.security import verify_access_token
from app.services import item_service

router = APIRouter(tags=["ITEMS"])

@router.get("/getItemcode_1_0", description="Get item code for given item name")
def get_itemcode(itemName: str, nature: Optional[str] = None, domain: Optional[str] = None, partCode: Optional[str] = None, made: Optional[str] = None):
    return item_service.get_itemcode(itemName, nature, domain, partCode, made)

@router.post("/addNewItem_1_0", description="Add new item to inventory")
async def add_new_item(item: str = Form(...), file: Optional[UploadFile] = File(None)):
    return await item_service.add_new_item(item, file)

@router.get("/getItemsList_1_0", description="get items master list")
def get_items_list(page: Optional[int] = Query(None, ge=1), pageSize: Optional[int] = Query(None, ge=1)):
    return item_service.get_items_list(page, pageSize)

@router.get("/getItemDetails_1_0/{itemId}", description="get items more details")
def get_item_details(itemId: int):
    return item_service.get_item_details(itemId)

@router.get("/getDistinctItem_1_0", description="Get distinct values for item attributes")
def get_distinct_item():
    return item_service.get_distinct_item()

@router.put("/updateItem_1_0", description="Update item with smart sync logic")
async def update_item(itemUpdateRequest: str = Form(...), file: Optional[UploadFile] = File(None)):
    return await item_service.update_item(itemUpdateRequest, file)

@router.get("/getAllInventoryItems_1_0", description="Get all physical inventory items")
def get_all_inventory_items(pageNo: int = Query(1, ge=1), pageSize: int = Query(10, ge=1), search: Optional[str] = None):
    return item_service.get_all_inventory_items(pageNo, pageSize, search)

@router.get("/getInventoryItemDetails_1_0", description="Get detailed view of a physical inventory item")
def get_inventory_item_details(purchaseItemId: int):
    return item_service.get_inventory_item_details(purchaseItemId)