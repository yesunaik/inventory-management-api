from fastapi import APIRouter, Query, Depends
#from app.core.security import verify_access_token
from app.services import site_service

router = APIRouter(tags=["SITES"])

@router.get("/getSiteInventory_1_0", description="get the inventory available in each site")
def get_site_inventory(siteId: int, viewType: str = Query("NORMAL")):
    return site_service.get_site_inventory(siteId, viewType)

@router.get("/getSiteInventoryItemDetails_1_0")
def get_site_inventory_item_details(siteId: int, itemId: int, type: str):
    return site_service.get_site_inventory_item_details(siteId, itemId, type)