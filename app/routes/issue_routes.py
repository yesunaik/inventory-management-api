from typing import Optional
from fastapi import APIRouter, Query, Depends
from datetime import date
#from app.core.security import verify_access_token
from app.services import issue_service
from app.schemas.inventory_schemas import CreateIssueModel, UpdateIssueStatusModel

router = APIRouter(tags=["ISSUED"])

@router.get("/getSitesByStore_1_0")
def get_sites_by_store(storeName: str):
    return issue_service.get_sites_by_store(storeName)

@router.get("/itemsForIssue_1_0", description="Get all available items for issue")
async def items_for_issue(storeId: int = None):
    return await issue_service.items_for_issue(storeId)

@router.get("/getProductsForIssue_1_0", description="get available products for issue")
def get_products_for_issue():
    return issue_service.get_products_for_issue()

@router.post("/createIssue_1_0", description="Issue stock items and products")
def create_issue(data: CreateIssueModel):
    return issue_service.create_issue(data)

@router.get("/getIssuedList_1_0", description="Get issued list")
def get_issued_list(
    pageNo: int = Query(1, ge=1), 
    pageSize: int = Query(15, ge=1), 
    search: str = None,
    fromDate: date = None, 
    toDate: date = None, 
    status: str = None, 
    storeId: int = None
):
    return issue_service.get_issued_list(pageNo, pageSize, search, fromDate, toDate, status, storeId)

@router.get("/getIssueDetails_1_0", description="get details of issued items")
def get_issue_details(issueId: int):
    return issue_service.get_issue_details(issueId)

@router.put("/updateIssueStatus_1_0", description="Update issue status and delivery details")
def update_issue_status(issueId: int, data: UpdateIssueStatusModel):
    return issue_service.update_issue_status(issueId, data)