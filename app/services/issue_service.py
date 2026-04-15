from typing import Optional
from datetime import datetime, date
from app.db.db import dbconn_inventory
from app.schemas.inventory_schemas import CreateIssueModel, UpdateIssueStatusModel

def get_sites_by_store(storeName: str):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
        SELECT siteId, siteName, 'site' AS entityType FROM vip_sites_management.sites WHERE status = 'Active' AND country = ( SELECT country FROM locations WHERE name = %s LIMIT 1 )
        """, (storeName,))
        rows = cursor.fetchall()
        return {"status": "Success", "statusCode": 200, "message": "Sites retrieved successfully", "data": rows}
    except Exception as e:
        return {"status": "Failed", "statusCode": 500, "message": f"Error fetching sites: {str(e)}", "data": []}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

async def items_for_issue(storeId: int = None):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        conditions = ["i.active = 'T'", "pi.status = 'DELIVERED'"]
        params = []
        if storeId:
            conditions.append("pinv.purchaseToId = %s")
            params.append(storeId)

        where_clause = " AND ".join(conditions)
        query = f"""
        SELECT i.id, i.itemName, i.make, i.model, i.itemCode, pi.serialNumber, pi.barcode, mdu.value AS units, COUNT(pi.id) AS quantity FROM items i JOIN purchase_items pi ON pi.itemId = i.id JOIN purchase_invoices pinv ON pinv.id = pi.purchaseId JOIN ( SELECT key_id,value FROM metadata.metadata_details WHERE metadata_types_id = (SELECT id FROM metadata.metadata_master WHERE type = 'Inv_Units') ) mdu ON mdu.key_id = i.units WHERE {where_clause} GROUP BY i.id, i.itemName, i.make, i.model, i.itemCode, pi.serialNumber, pi.barcode, mdu.value ORDER BY i.id
        """
        cursor.execute(query, params)
        return {"status": "Success", "statusCode": 200, "message": "Items fetched successfully", "data": cursor.fetchall()}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": "Error while fetching items", "data": str(e)}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_products_for_issue():
    conn = None
    cursor = None
    try :
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        query = """select pd.productId,pd.id as productDetailsId,p.productName,pd.serialNumber,pd.barCode,pd.quantity,mdps.value as productStatus,p.make,p.model,mdu.value as units from product_details pd LEFT join products p on p.id = pd.productId LEFT join (select key_id,value from metadata.metadata_details where metadata_types_id = (select id FROM metadata.metadata_master where type = "Inv_Units")) mdu on mdu.key_id = p.units LEFT JOIN (select key_id,value from metadata.metadata_details where metadata_types_id = (select id FROM metadata.metadata_master where type = "Inv_productStatus")) mdps on mdps.key_id = pd.statusId where pd.statusId in (1,4)"""
        cursor.execute(query)
        return {"status":"success","statusCode":200,"message":"data retrived successfully","data":cursor.fetchall()}
    except Exception as e:
        if conn: conn.rollback()
        return {"status":"failed","statusCode":500,"message":f"error while retriving data {str(e)}","data":None}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def create_issue(data: CreateIssueModel):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        createdTime = data.createdTime or datetime.now()

        if not data.items and not data.products: return {"status":"Failed", "statusCode":400, "message":"At least one item or product required"}
        if data.status == "ISSUED" and data.deliveredDate: return {"status":"Failed", "statusCode":400, "message":"Delivered date should not be given for ISSUED status"}
        if data.status == "DELIVERED":
            if not data.shipmentDate or not data.deliveredDate: return {"status":"Failed", "statusCode":400, "message":"ShipmentDate and DeliveredDate required"}
            if data.deliveredDate < data.shipmentDate: return {"status":"Failed", "statusCode":400, "message":"Delivered date must be greater than shipment date"}

        cursor.execute("""
        INSERT INTO issued ( issueDate, issuedFromId, issuedToId, categoryId, transportationId, billingTypeId, trackingId, shipmentDate, deliveredDate, status, createdBy, createdTime, remarks ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (data.issueDate, data.issuedFromId, data.issuedToId, data.categoryId, data.transportationId, data.billingTypeId, data.trackingId, data.shipmentDate, data.deliveredDate, data.status, data.createdBy, createdTime, data.remarks))
        issue_id = cursor.lastrowid

        for item in data.items:
            if item.serialNumber and item.barcode:
                cursor.execute("SELECT id,itemId,status FROM purchase_items WHERE serialNumber=%s AND barcode=%s", (item.serialNumber,item.barcode))
                purchase = cursor.fetchone()
                if not purchase: return {"status":"Failed", "statusCode":404, "message":f"Item {item.serialNumber} not found"}
                if purchase["status"] != "DELIVERED": return {"status":"Failed", "statusCode":400, "message":"Item already issued/used"}
                if purchase["itemId"] != item.itemId: return {"status":"Failed", "statusCode":400, "message":"Item mismatch"}

                cursor.execute("""INSERT INTO issue_items ( issueId, itemId, quantity, serialNumber, barcode, billingTypeId, status, createdBy, createdTime ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (issue_id, item.itemId, 1, item.serialNumber, item.barcode, item.billingTypeId, data.status, data.createdBy, createdTime))
                issueItemId = cursor.lastrowid

                cursor.execute("""INSERT INTO stock_ledger ( itemId, stockHolderId, action, actionId, actionItemId, qtyIn, qtyOut, createdBy, createdTime ) VALUES (%s,%s,'ISSUE',%s,%s,0,%s,%s,%s)""",
                    (item.itemId, data.issuedFromId, issue_id, issueItemId, 1, data.createdBy, createdTime))
                cursor.execute("UPDATE purchase_items SET status='ISSUED', modifiedBy=%s, modifiedTime=%s WHERE id=%s", (data.createdBy, createdTime, purchase["id"]))
            else:
                if not item.quantity: return {"status":"Failed", "statusCode":400, "message":"Quantity required for non serial item"}
                cursor.execute("SELECT id FROM purchase_items WHERE itemId=%s AND status='DELIVERED' LIMIT %s", (item.itemId,item.quantity))
                purchase_rows = cursor.fetchall()
                if len(purchase_rows) < item.quantity: return {"status":"Failed", "statusCode":400, "message":"Not enough stock available"}

                cursor.execute("""INSERT INTO issue_items ( issueId, itemId, quantity, billingTypeId, status, createdBy, createdTime ) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (issue_id, item.itemId, item.quantity, item.billingTypeId, data.status, data.createdBy, createdTime))
                issueItemId = cursor.lastrowid

                cursor.execute("""INSERT INTO stock_ledger ( itemId, stockHolderId, action, actionId, actionItemId, qtyIn, qtyOut, createdBy, createdTime ) VALUES (%s,%s,'ISSUE',%s,%s,0,%s,%s,%s)""",
                    (item.itemId, data.issuedFromId, issue_id, issueItemId, item.quantity, data.createdBy, createdTime))

                for r in purchase_rows:
                    cursor.execute("UPDATE purchase_items SET status='ISSUED', modifiedBy=%s, modifiedTime=%s WHERE id=%s", (data.createdBy, createdTime, r["id"]))

        for prod in data.products:
            cursor.execute("SELECT id,statusId FROM product_details WHERE id=%s", (prod.productDetailsId,))
            product = cursor.fetchone()
            if not product: return {"status":"Failed", "statusCode":404, "message":"Product not found"}

            cursor.execute("""INSERT INTO issue_products ( issueId, productDetailsId, quantity, billingTypeId, status, createdBy, createdTime ) VALUES (%s,%s,1,%s,%s,%s,%s)""",
                (issue_id, prod.productDetailsId, prod.billingTypeId, prod.productStatus, data.createdBy, createdTime))

            new_status = 2 if prod.productStatus == "SALE" else 3
            cursor.execute("UPDATE product_details SET statusId=%s, currentLocationId=%s, modifiedBy=%s, modifiedTime=%s WHERE id=%s",
                (new_status, data.issuedToId, data.createdBy, createdTime, prod.productDetailsId))

        conn.commit()
        return {"status":"Success", "statusCode":200, "message":"Issue created successfully", "data":{"issueId":issue_id}}
    except Exception as e:
        if conn: conn.rollback()
        return {"status":"Failed", "statusCode":500, "message":f"Error while creating issue: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_issued_list(pageNo: int, pageSize: int, search: str, fromDate: date, toDate: date, status: str, storeId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        offset = (pageNo - 1) * pageSize
        where_clauses, params = [], []

        if search:
            where_clauses.append("(l.name LIKE %s OR ls.name LIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
        if fromDate:
            where_clauses.append("iss.issueDate >= %s")
            params.append(fromDate)
        if toDate:
            where_clauses.append("iss.issueDate <= %s")
            params.append(toDate)
        if status:
            where_clauses.append("iss.status = %s")
            params.append(status)
        if storeId:
            where_clauses.append("iss.issuedFromId = %s")
            params.append(storeId)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        cursor.execute(f"SELECT COUNT(*) as totalRecords FROM issued iss LEFT JOIN locations l ON l.id = iss.issuedFromId LEFT JOIN locations ls ON ls.id = iss.issuedToId {where_sql}", tuple(params))
        totalRecords = cursor.fetchone()["totalRecords"]

        data_query = f"""SELECT iss.id, DATE_FORMAT(iss.issueDate, '%d/%m/%Y') as issueDate, l.name as issuedFrom,iss.issuedFromId, ls.name as issuedTo,iss.issuedToId, COALESCE(mdc.value, 'N/A') as category, COALESCE(mdt.value, 'N/A') as transportation,COALESCE(mdb.value, 'N/A') as billing, iss.status FROM issued iss LEFT JOIN locations l ON l.id = iss.issuedFromId LEFT JOIN (SELECT * FROM (SELECT id, entityType, name, country, status FROM locations UNION ALL SELECT siteId, 'site', siteName, country, status FROM vip_sites_management.sites) AS merged WHERE status IN ('T', 'Active')) ls ON ls.id = iss.issuedToId LEFT JOIN metadata.metadata_master mmc ON TRIM(mmc.type) = 'Inv_Category' LEFT JOIN metadata.metadata_details mdc ON mdc.key_id = iss.categoryId AND mdc.metadata_types_id = mmc.id LEFT JOIN metadata.metadata_master mmt ON TRIM(mmt.type) = 'Inv_ShippingFlatform' LEFT JOIN metadata.metadata_details mdt ON mdt.key_id = iss.transportationId AND mdt.metadata_types_id = mmt.id LEFT JOIN metadata.metadata_master mmb ON TRIM(mmb.type) = 'Inv_BillingStatus' LEFT JOIN metadata.metadata_details mdb ON mdb.key_id = iss.billingTypeId AND mdb.metadata_types_id = mmb.id {where_sql} ORDER BY iss.id DESC LIMIT %s OFFSET %s """
        cursor.execute(data_query, tuple(params + [pageSize, offset]))

        return { "status": "Success", "statusCode": 200, "data": cursor.fetchall(), "pagination": { "pageNo": pageNo, "pageSize": pageSize,"totalRecords": totalRecords, "totalPages": (totalRecords + pageSize - 1) // pageSize }}
    except Exception as e:
        if conn: conn.rollback()
        return { "status": "Failed","statusCode": 500, "message": f"Error while fetching issued list: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_issue_details(issueId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""SELECT iss.id, DATE_FORMAT(iss.issueDate,'%d %b, %Y') as issueDate, lf.name as issuedFrom,lt.name as issuedTo, mdc.value as category, mdt.value as transportation,mdb.value as billing, iss.trackingId, DATE_FORMAT(iss.shipmentDate,'%d %b, %Y') as shipmentDate, DATE_FORMAT(iss.deliveredDate,'%d %b, %Y') as deliveredDate, iss.status FROM issued iss LEFT JOIN (SELECT * FROM (SELECT id, entityType, name, country, status FROM locations UNION ALL SELECT siteId, 'site', siteName, country, status FROM vip_sites_management.sites) AS merged WHERE status IN ('T', 'Active')) lf ON lf.id = iss.issuedFromId LEFT JOIN (SELECT * FROM (SELECT id, entityType, name, country, status FROM locations UNION ALL SELECT siteId, 'site', siteName, country, status FROM vip_sites_management.sites) AS merged WHERE status IN ('T', 'Active')) lt ON lt.id = iss.issuedToId LEFT JOIN metadata.metadata_details mdc ON mdc.key_id = iss.categoryId AND mdc.metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = "Inv_Category" ) LEFT JOIN metadata.metadata_details mdt ON mdt.key_id = iss.transportationId AND mdt.metadata_types_id = (SELECT id FROM metadata.metadata_master WHERE type = "Inv_ShippingFlatform") LEFT JOIN metadata.metadata_details mdb ON mdb.key_id = iss.billingTypeId AND mdb.metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = "Inv_BillingStatus" ) WHERE iss.id= %s""", (issueId,))
        header = cursor.fetchone()
        if not header: return {"status": "Failed","statusCode": 404, "message": "Issue not found","data": {}}

        cursor.execute("""
            SELECT ii.id AS issueItemId, i.itemName, i.make, i.model, ii.quantity, u.value as units, mdb.value as billingStatus, ii.serialNumber, ii.barcode, lt.name as assignSite FROM issue_items ii LEFT JOIN items i ON i.id = ii.itemId LEFT JOIN (SELECT * FROM (SELECT id, entityType, name, country, status FROM locations UNION ALL SELECT siteId, 'site', siteName, country, status FROM vip_sites_management.sites ) AS merged WHERE status IN ('T', 'Active')) lt ON lt.id = ( SELECT issuedToId FROM issued WHERE id = ii.issueId ) LEFT JOIN metadata.metadata_details u ON u.key_id = i.units AND u.metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = "Inv_Units" ) LEFT JOIN metadata.metadata_details mdb ON mdb.key_id = ii.billingTypeId AND mdb.metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = "Inv_BillingStatus" ) WHERE ii.issueId=%s
        """, (issueId,))
        items = cursor.fetchall()

        cursor.execute("""SELECT ip.id AS issueProductId, pd.id as productDetailsId, p.productName, ip.quantity, u.value as units, mdb.value as billingStatus FROM issue_products ip LEFT JOIN product_details pd ON pd.id = ip.productDetailsId LEFT JOIN products p ON p.id = pd.productId LEFT JOIN metadata.metadata_details u ON u.key_id = p.units AND u.metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = "Inv_Units" ) LEFT JOIN metadata.metadata_details mdb ON mdb.key_id = ip.billingTypeId AND mdb.metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = "Inv_BillingStatus" ) WHERE ip.issueId=%s""", (issueId,))
        products = cursor.fetchall()

        for product in products:
            cursor.execute("""
                SELECT i.itemName, i.make, i.model, pi.itemsQuantity, u.value as units FROM product_items pi LEFT JOIN items i ON i.id = pi.itemsId LEFT JOIN metadata.metadata_details u ON u.key_id = pi.itemsUnits AND u.metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = "Inv_Units" ) WHERE pi.productId = ( SELECT productId FROM product_details WHERE id=%s )
            """, (product["productDetailsId"],))
            product["hardware"] = cursor.fetchall()

        return {"status": "Success", "statusCode": 200,"message":"data fected successfully", "data":{ "header": header, "items": items, "products": products}}
    except Exception as e:
        if conn: conn.rollback()
        return { "status": "Failed","statusCode": 500, "message":f" error while fetching data {str(e)}","data": {}}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def update_issue_status(issueId: int, data: UpdateIssueStatusModel):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        modifiedTime = datetime.now()

        cursor.execute("SELECT status FROM issued WHERE id=%s", (issueId,))
        issue = cursor.fetchone()
        if not issue: return {"status": "Failed", "statusCode": 404, "message": "Issue not found"}
        if issue["status"] == "CANCELLED": return {"status": "Failed", "statusCode": 400, "message": "Cannot update cancelled issue"}

        if data.status == "DELIVERED":
            if not data.shipmentDate: return {"status":"Failed", "statusCode":400, "message":"Shipment date required"}
            if not data.deliveredDate: return {"status":"Failed", "statusCode":400, "message":"Delivered date required"}
            if data.deliveredDate < data.shipmentDate: return {"status":"Failed", "statusCode":400, "message":"Delivered date must be greater than shipment date"}

        cursor.execute("""
        UPDATE issued SET status=%s, transportationId=%s, trackingId=%s, shipmentDate=%s, deliveredDate=%s, modifiedBy=%s, modifiedTime=%s WHERE id=%s
        """, (data.status, data.transportationId, data.trackingId, data.shipmentDate, data.deliveredDate, data.modifiedBy, modifiedTime, issueId))

        if data.status == "DELIVERED":
            cursor.execute("UPDATE issue_items SET status='DELIVERED', modifiedBy=%s, modifiedTime=%s WHERE issueId=%s", (data.modifiedBy, modifiedTime, issueId))

        conn.commit()
        return {"status": "Success", "statusCode": 200, "message": "Issue updated successfully"}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"Error while updating issue: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()