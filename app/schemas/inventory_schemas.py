from pydantic import BaseModel
from typing import List, Optional, Literal
from datetime import datetime, date

class ItemSaveRequest(BaseModel):
    itemName: str
    units: Optional[str] = None
    make: Optional[str] = None
    model: Optional[str] = None
    usedForIds: Optional[List[int]] = None
    itemCode: str
    serialNumberFlag: str = "T"
    barcodeFlag: str = "T"
    purchaseItemLinks: Optional[List[str]] = None
    createdBy: int
    createdTime: Optional[datetime] = None

class ItemUpdateRequest(BaseModel):
    itemId: int
    usedForIds: Optional[List[int]] = None
    purchaseItemLinks: Optional[List[str]] = None
    remarks: Optional[str] = None
    modifiedBy: int
    modifiedTime: Optional[datetime] = None

class ProductComponent(BaseModel):
    itemId: int
    itemsQuantity: int
    itemUnitId: int

class ProductSaveRequest(BaseModel):
    productName: str
    productCode: str
    ProductUnitId: int
    make: str
    model: str
    description: Optional[str] = None
    publishedDate: str
    useForIds: List[int]
    itemsList: List[ProductComponent]
    createdBy: int
    createdTime: Optional[datetime] = None

class ItemUsedModel(BaseModel):
    itemId: int
    purchaseItemIds: Optional[List[int]] = []
    quantity: Optional[int] = 0

class CreateProductModel(BaseModel):
    productId: int
    serialNumber: str
    barCode: str
    currentLocationId: int
    remarks: Optional[str] = None
    itemsUsed: List[ItemUsedModel]
    createdBy: int
    createdTime: Optional[datetime] = None

class UpdateProductModel(BaseModel):
    productDetailId: int
    currentLocationId: Optional[int] = None
    statusId: Optional[int] = None
    remarks: Optional[str] = None
    publishedDate: Optional[date] = None
    usedFor: Optional[List[int]] = []
    modifiedBy: int

class PurchaseItemModel(BaseModel):
    itemId: int
    quantity: int
    unitPrice: float
    gstPercent: float
    serialNumbers: Optional[List[str]] = None
    barcodes: Optional[List[str]] = None

class PurchaseCreateModel(BaseModel):
    invoiceNumber: Optional[str] = None
    purchaseFromId: int
    purchaseToId: int
    purchaseType: Optional[str] = "ONLINE"
    invoiceDate: date
    invoiceType: str
    items: List[PurchaseItemModel]
    createdBy: int
    createdTime: Optional[datetime] = None

class PurchaseItemUpdateModel(BaseModel):
    purchaseItemId: int
    status: Literal["DELIVERED","RETURNED"]
    serialNumber: Optional[str] = None
    barcode: Optional[str] = None
    returnReason: Optional[str] = None

class PurchaseUpdateModel(BaseModel):
    purchaseId: int
    invoiceNumber: Optional[str] = None
    items: List[PurchaseItemUpdateModel]
    modifiedBy: int
    modifiedTime: Optional[datetime] = None

class IssueItemModel(BaseModel):
    itemId: int
    quantity: Optional[int] = None
    serialNumber: Optional[str] = None
    barcode: Optional[str] = None
    billingTypeId: int

class IssueProductModel(BaseModel):
    productDetailsId: int
    productStatus: Literal["SALE","LEASE"]
    billingTypeId: int

class CreateIssueModel(BaseModel):
    issueDate: date
    issuedFromId: int
    issuedToId: int
    categoryId: int
    billingTypeId: int
    status: Literal["ISSUED","DELIVERED"]
    transportationId: Optional[int] = None
    trackingId: Optional[str] = None
    shipmentDate: Optional[date] = None
    deliveredDate: Optional[date] = None
    remarks: Optional[str] = None
    items: List[IssueItemModel] = []
    products: List[IssueProductModel] = []
    createdBy: int
    createdTime: Optional[datetime] = None

class UpdateIssueStatusModel(BaseModel):
    status: Literal["DELIVERED","RETURNED"]
    transportationId: Optional[int] = None
    trackingId: Optional[str] = None
    shipmentDate: Optional[date] = None
    deliveredDate: Optional[date] = None
    modifiedBy: int

class ReturnItemModel(BaseModel):
    issueItemId: int
    itemId: int
    conditionType: Literal["USED","SCRAP"]

class ReturnProductModel(BaseModel):
    issueProductId: int
    productDetailsId: int
    conditionType: Literal["USED","SCRAP"]

class CreateReturnModel(BaseModel):
    returnDate: date
    returnFromId: int
    returnToId: int
    status: Literal["RETURNED","IN_TRANSIT"]
    remarks: Optional[str] = None
    items: List[ReturnItemModel] = []
    products: List[ReturnProductModel] = []
    createdBy: int

class UpdateReturnItemModel(BaseModel):
    returnItemId: int
    conditionType: Literal["USED","SCRAP"]

class UpdateReturnProductModel(BaseModel):
    returnProductId: int
    conditionType: Literal["USED","SCRAP"]

class UpdateReturnModel(BaseModel):
    returnDate: Optional[date] = None
    status: Literal["IN_TRANSIT","RETURNED"]
    remarks: Optional[str] = None
    items: List[UpdateReturnItemModel] = []
    products: List[UpdateReturnProductModel] = []
    modifiedBy: int