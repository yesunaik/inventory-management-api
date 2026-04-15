from typing import Optional
from datetime import datetime, date
from app.db.db import dbconn_inventory
from app.schemas.inventory_schemas import CreateReturnModel, UpdateReturnModel

def get_returnable_stock(returnFromId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
        SELECT ii.id AS issueItemId, ii.itemId, i.itemName, i.make, i.model, ii.serialNumber, ii.barcode, ii.quantity, iss.id AS issueId FROM issue_items ii JOIN issued iss ON iss.id = ii.issueId JOIN items i ON i.id = ii.itemId WHERE iss.issuedToId = %s AND iss.status IN ('ISSUED','DELIVERED','PARTIAL') AND ii.status IN ('ISSUED','DELIVERED') AND NOT EXISTS ( SELECT 1 FROM return_items ri WHERE ri.issueItemId = ii.id )
        """, (returnFromId,))
        items = cursor.fetchall()

        cursor.execute("""
        SELECT ip.id AS issueProductId, ip.productDetailsId, p.productName, p.make, p.model, pd.serialNumber, pd.barCode, ip.quantity, ip.status AS productStatus, iss.id AS issueId FROM issue_products ip JOIN issued iss ON iss.id = ip.issueId JOIN product_details pd ON pd.id = ip.productDetailsId JOIN products p ON p.id = pd.productId WHERE iss.issuedToId = %s AND iss.status IN ('ISSUED','DELIVERED','PARTIAL') AND ip.status IN ('SALE','LEASE') AND NOT EXISTS ( SELECT 1 FROM return_products rp WHERE rp.issueProductId = ip.id )
        """, (returnFromId,))
        products = cursor.fetchall()

        return {"status": "Success", "statusCode": 200, "data": {"items": items, "products": products}}
    except Exception as e:
        return {"status": "Failed", "statusCode": 500, "message": f"error while getting returning item: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def add_return(data: CreateReturnModel):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        createdTime = datetime.now()

        cursor.execute("INSERT INTO returns (returnDate, returnFromId, returnToId, status, createdBy, createdTime, remarks) VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (data.returnDate, data.returnFromId, data.returnToId, data.status, data.createdBy, createdTime, data.remarks))
        return_id = cursor.lastrowid

        for item in data.items:
            cursor.execute("SELECT ii.id, ii.itemId, ii.serialNumber, ii.barcode FROM issue_items ii JOIN issued iss ON iss.id = ii.issueId WHERE ii.id=%s AND iss.issuedToId=%s AND ii.status='DELIVERED'", (item.issueItemId, data.returnFromId))
            issue_item = cursor.fetchone()
            if not issue_item: return {"status":"Failed", "statusCode":400, "message":f"Issue item {item.issueItemId} not valid for return"}

            cursor.execute("SELECT id FROM return_items WHERE issueItemId=%s",(item.issueItemId,))
            if cursor.fetchone(): return {"status":"Failed", "statusCode":400, "message":f"Issue item {item.issueItemId} already returned"}

            cursor.execute("INSERT INTO return_items (returnId, issueItemId, itemId, conditionType, createdBy, createdTime) VALUES (%s,%s,%s,%s,%s,%s)", (return_id, item.issueItemId, item.itemId, item.conditionType, data.createdBy, createdTime))
            cursor.execute("UPDATE issue_items SET status='RETURNED' WHERE id=%s",(item.issueItemId,))

            if item.conditionType == "USED":
                cursor.execute("INSERT INTO stock_ledger (itemId, stockHolderId, action, qtyIn, qtyOut, createdBy, createdTime) VALUES (%s,%s,'RETURN',1,0,%s,%s)", (issue_item["itemId"], data.returnToId, data.createdBy, createdTime))
                cursor.execute("UPDATE purchase_items SET status='DELIVERED', modifiedBy=%s, modifiedTime=%s WHERE serialNumber=%s", (data.createdBy, createdTime, issue_item["serialNumber"]))
            elif item.conditionType == "SCRAP":
                cursor.execute("UPDATE purchase_items SET status='SCRAP', modifiedBy=%s, modifiedTime=%s WHERE serialNumber=%s", (data.createdBy, createdTime, issue_item["serialNumber"]))

        for prod in data.products:
            cursor.execute("SELECT ip.productDetailsId FROM issue_products ip JOIN issued iss ON iss.id = ip.issueId WHERE ip.id=%s AND iss.issuedToId=%s", (prod.issueProductId, data.returnFromId))
            issue_product = cursor.fetchone()
            if not issue_product: return {"status":"Failed", "statusCode":400, "message":f"Issue product {prod.issueProductId} not valid for return"}

            cursor.execute("SELECT id FROM return_products WHERE issueProductId=%s",(prod.issueProductId,))
            if cursor.fetchone(): return {"status":"Failed", "statusCode":400, "message":f"Issue product {prod.issueProductId} already returned"}

            cursor.execute("INSERT INTO return_products (returnId, issueProductId, productDetailsId, conditionType, createdBy, createdTime) VALUES (%s,%s,%s,%s,%s,%s)", (return_id, prod.issueProductId, prod.productDetailsId, prod.conditionType, data.createdBy, createdTime))
            cursor.execute("UPDATE issue_products SET status='RETURNED' WHERE id=%s",(prod.issueProductId,))

            new_status = 4 if prod.conditionType == "USED" else 5
            cursor.execute("UPDATE product_details SET statusId=%s, currentLocationId=%s, modifiedBy=%s, modifiedTime=%s WHERE id=%s", (new_status, data.returnToId, data.createdBy, createdTime, issue_product["productDetailsId"]))

        conn.commit()
        return {"status":"Success", "statusCode":200, "message":"Return created successfully", "data":{"returnId":return_id}}
    except Exception as e:
        if conn: conn.rollback()
        return {"status":"Failed", "statusCode":500, "message":f"error in creating return: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_return_list(startDate: date, endDate: date, pageNo: int, pageSize: int, search: str, status: str, storeId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        offset = (pageNo - 1) * pageSize
        where_clauses, params = [], []

        if startDate and endDate:
            where_clauses.append("DATE(r.returnDate) BETWEEN %s AND %s")
            params.extend([startDate, endDate])
        if search:
            where_clauses.append("(lf.name LIKE %s OR lt.name LIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
        if status:
            where_clauses.append("r.status = %s")
            params.append(status)
        if storeId:
            where_clauses.append("r.returnToId = %s")
            params.append(storeId)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        cursor.execute(f"SELECT COUNT(DISTINCT r.id) AS totalRecords FROM returns r LEFT JOIN locations lf ON lf.id = r.returnFromId LEFT JOIN locations lt ON lt.id = r.returnToId {where_sql}", tuple(params))
        totalRecords = cursor.fetchone()["totalRecords"]

        data_query = f"""SELECT r.id,DATE_FORMAT(r.returnDate, '%d/%m/%Y') AS returnDate,lf.id AS returnFromId,lf.name AS returnFrom, lt.id AS returnToId,lt.name AS returnTo, r.status,(SELECT COUNT(*) FROM return_items ri WHERE ri.returnId = r.id) + ( SELECT COUNT(*) FROM return_products rp WHERE rp.returnId = r.id) AS totalEntries FROM returns r LEFT JOIN (SELECT * FROM (SELECT id, entityType, name, country, status FROM locations UNION ALL SELECT siteId, 'site', siteName, country, status FROM vip_sites_management.sites ) AS merged WHERE status IN ('T', 'Active')) lf ON lf.id = r.returnFromId LEFT JOIN locations lt ON lt.id = r.returnToId {where_sql} ORDER BY r.id DESC LIMIT %s OFFSET %s"""
        cursor.execute(data_query, tuple(params + [pageSize, offset]))

        return { "status": "Success", "statusCode": 200, "data": cursor.fetchall(), "pagination": { "pageNo": pageNo,"pageSize": pageSize, "totalRecords": totalRecords, "totalPages": (totalRecords + pageSize - 1) // pageSize }}
    except Exception as e:
        return {"status": "Failed", "statusCode": 500, "message": f"error while retriving the return list {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_return_details(returnId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""SELECT r.id, DATE_FORMAT(r.returnDate,'%d %b, %Y') AS returnDate,lf.id as returnFromId,lf.name AS returnFrom, lt.id as returnToId ,lt.name AS returnTo,r.status, r.remarks FROM returns r LEFT JOIN (SELECT * FROM (SELECT id, entityType, name, country, status FROM locations UNION ALL SELECT siteId, 'site', siteName, country, status FROM vip_sites_management.sites ) AS merged WHERE status IN ('T', 'Active')) lf ON lf.id = r.returnFromId LEFT JOIN (SELECT * FROM (SELECT id, entityType, name, country, status FROM locations UNION ALL SELECT siteId, 'site', siteName, country, status FROM vip_sites_management.sites ) AS merged WHERE status IN ('T', 'Active')) lt ON lt.id = r.returnToId WHERE r.id = %s """, (returnId,))
        header = cursor.fetchone()
        if not header: return { "status": "Failed","statusCode": 404,"message": "Return not found"}

        cursor.execute("""SELECT ri.id, i.itemName, ii.serialNumber, ii.barcode,ii.quantity, ri.conditionType, md.value AS billingType FROM return_items ri JOIN issue_items ii ON ii.id = ri.issueItemId JOIN items i ON i.id = ii.itemId LEFT JOIN metadata.metadata_details md ON md.key_id = ii.billingTypeId AND md.metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = 'Inv_BillingStatus' ) WHERE ri.returnId = %s """, (returnId,))
        items = cursor.fetchall()

        cursor.execute("""SELECT rp.id, p.productName, pd.serialNumber,pd.barCode, p.make, p.model,ip.quantity, rp.conditionType FROM return_products rp JOIN issue_products ip ON ip.id = rp.issueProductId JOIN product_details pd ON pd.id = ip.productDetailsId JOIN products p ON p.id = pd.productId WHERE rp.returnId = %s """, (returnId,))
        products = cursor.fetchall()

        return {"status": "Success", "statusCode": 200, "data": { "header": header,"items": items,"products": products}}
    except Exception as e:
        return { "status": "Failed", "statusCode": 500, "message": f"Error while fetching return details: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn:  conn.close()

def update_return(returnId: int, data: UpdateReturnModel):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        modifiedTime = datetime.now()

        cursor.execute("SELECT status FROM returns WHERE id=%s",(returnId,))
        existing = cursor.fetchone()
        if not existing: return {"status":"Failed", "statusCode":404, "message":"Return not found"}

        current_status = existing["status"]
        if current_status == "DELIVERED": return {"status":"Failed", "statusCode":400, "message":"Return already delivered"}
        if current_status == "IN_TRANSIT" and data.status not in ["IN_TRANSIT","RETURNED"]: return {"status":"Failed", "statusCode":400, "message":"Invalid status transition"}

        cursor.execute("UPDATE returns SET returnDate = COALESCE(%s, returnDate), status = %s, remarks = %s, modifiedBy = %s, modifiedTime = %s WHERE id = %s", (data.returnDate, data.status, data.remarks, data.modifiedBy, modifiedTime, returnId))

        for item in data.items:
            cursor.execute("UPDATE return_items SET conditionType=%s WHERE id=%s AND returnId=%s", (item.conditionType, item.returnItemId, returnId))
        for prod in data.products:
            cursor.execute("UPDATE return_products SET conditionType=%s WHERE id=%s AND returnId=%s", (prod.conditionType, prod.returnProductId, returnId))

        if data.status == "DELIVERED":
            cursor.execute("SELECT ri.conditionType, ii.itemId, ii.serialNumber FROM return_items ri JOIN issue_items ii ON ii.id = ri.issueItemId WHERE ri.returnId=%s", (returnId,))
            return_items = cursor.fetchall()
            for item in return_items:
                if item["conditionType"] == "USED":
                    cursor.execute("INSERT INTO stock_ledger (itemId,action,qtyIn,qtyOut,createdBy,createdTime) VALUES (%s,'RETURN',1,0,%s,%s)", (item["itemId"], data.modifiedBy, modifiedTime))
                    if item["serialNumber"]: cursor.execute("UPDATE purchase_items SET status= 'USED', modifiedBy=%s, modifiedTime=%s WHERE serialNumber=%s", (data.modifiedBy, modifiedTime, item["serialNumber"]))
                elif item["conditionType"] == "SCRAP":
                    if item["serialNumber"]: cursor.execute("UPDATE purchase_items SET status='SCRAP', modifiedBy=%s, modifiedTime=%s WHERE serialNumber=%s", (data.modifiedBy, modifiedTime, item["serialNumber"]))

            cursor.execute("SELECT rp.conditionType, pd.id AS productDetailsId FROM return_products rp JOIN issue_products ip ON ip.id = rp.issueProductId JOIN product_details pd ON pd.id = ip.productDetailsId WHERE rp.returnId=%s", (returnId,))
            return_products = cursor.fetchall()
            for prod in return_products:
                new_status = 4 if prod["conditionType"] == "USED" else 5
                cursor.execute("UPDATE product_details SET statusId=%s, modifiedBy=%s, modifiedTime=%s WHERE id=%s", (new_status, data.modifiedBy, modifiedTime, prod["productDetailsId"]))

        conn.commit()
        return {"status":"Success", "statusCode":200, "message":"Return updated successfully"}
    except Exception as e:
        if conn: conn.rollback()
        return {"status":"Failed", "statusCode":500, "message":f"Error while updating return: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()