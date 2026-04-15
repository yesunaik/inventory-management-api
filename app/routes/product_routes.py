from typing import Optional, List
from fastapi import APIRouter, Query, UploadFile, File, Form, Depends
#from app.core.security import verify_access_token
from app.services import product_service
from app.schemas.inventory_schemas import ProductSaveRequest, UpdateProductModel

router = APIRouter(tags=["PRODUCTS"])

@router.get("/getProductcode_1_0", description="Get product code for given product name")
def get_productcode(productName: str, nature: Optional[str] = None, domain: Optional[str] = None, partCode: Optional[str] = None, made: Optional[str] = None):
    return product_service.get_productcode(productName, nature, domain, partCode, made)

@router.post("/createProduct_1_0", description="Add new product with components and useFor mapping")
def create_product(payload: ProductSaveRequest):
    return product_service.create_product(payload)

@router.get("/getProductsList_1_0", description="Get paginated list of products")
def get_products_list():
    return product_service.get_products_list()

@router.get("/getProductDetails_1_0", description= "get products more details")
def get_product_details(product_id: int):
    return product_service.get_product_details(product_id)

@router.get("/getAvailableItems_1_0", description="Get available items for a product")
def get_available_items(itemId: int = None, storeId: int = None):
    return product_service.get_available_items(itemId, storeId)

@router.get("/getCostOfItems_1_0", description="Get cost of items for a product")
def get_cost_of_items(productItemId: List[int] = Query(...)):
    return product_service.get_cost_of_items(productItemId)

@router.post("/addNewProduct_1_0", description="Create product using available items")
async def add_new_product(data: str = Form(...), file: Optional[UploadFile] = File(None)):
    return await product_service.add_new_product(data, file)

@router.get("/getAllProductsList_1_0", description="get product more details with items and quantities")
def get_all_products_list(pageNo: int = Query(1, ge=1), pageSize: int = Query(10, ge=1), search: str = None, statusId: int = None, locationId: int = None):
    return product_service.get_all_products_list(pageNo, pageSize, search, statusId, locationId)

@router.get("/getProductAllDetails_1_0")
def get_product_all_details(productDetailId: int):
    return product_service.get_product_all_details(productDetailId)

@router.put("/updateProduct_1_0")
def update_product(data: UpdateProductModel):
    return product_service.update_product(data)