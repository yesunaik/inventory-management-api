import json
from typing import Optional, List
from datetime import datetime
from app.db.db import dbconn_inventory
from app.utils.functions import upload_file_to_s3_handler
from app.schemas.inventory_schemas import ProductSaveRequest, CreateProductModel, UpdateProductModel

def get_productcode(productName: str, nature: Optional[str], domain: Optional[str], partCode: Optional[str], made: Optional[str]):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        if all([nature, domain, partCode, made]):
            base_code = f"{nature.strip()}-{domain.strip()}-{partCode.strip()}-{made.strip()}".upper()
            cursor.execute("""SELECT COALESCE(MAX(CAST(SUBSTRING_INDEX(productCode, '-', -1) AS UNSIGNED)),0) AS max_seq FROM products WHERE productCode LIKE %s """, (f"{base_code}-%",))
            next_seq = (cursor.fetchone()["max_seq"] or 0) + 1
            return {"status": "success","statusCode": 200,"message": "Product code generated successfully","productCode": f"{base_code}-{str(next_seq).zfill(5)}"}
        else:
            cursor.execute(""" SELECT productCode FROM products WHERE productName = %s ORDER BY id DESC LIMIT 1 """, (productName.strip(),))
            row = cursor.fetchone()
            if row and row["productCode"]:
                last_code = row["productCode"]
                prefix = last_code.rsplit("-", 1)[0]
                next_seq = int(last_code.split("-")[-1]) + 1
                return {"status": "success","statusCode": 200,"message": "Product code generated successfully","productCode": f"{prefix}-{str(next_seq).zfill(5)}"}
            else:
                return {"status": "failed","statusCode": 400,"message": "Base parameters required for new product","productCode": None}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "failed","statusCode": 500,"message": str(e),"productCode": None}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def create_product(payload: ProductSaveRequest):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        if not payload.itemsList: return {"status": "failed", "statusCode": 400, "message": "Product items cannot be empty", "data": None}

        cursor.execute("""INSERT INTO products (productName, productCode, units, make, model,description, publishedDate, active, createdBy, createdTime) VALUES (%s,%s,%s,%s,%s,%s,%s,'T',%s,%s) """, 
                        (payload.productName,payload.productCode,payload.ProductUnitId,payload.make,payload.model, payload.description,payload.publishedDate, payload.createdBy,payload.createdTime or datetime.now()))
        product_id = cursor.lastrowid

        for item in payload.itemsList:
            cursor.execute("""INSERT INTO product_items (productId, itemsId, itemsQuantity, itemsUnits, createdBy, createdTime) VALUES (%s,%s,%s,%s,%s,%s) """,
                            (product_id,item.itemId, item.itemsQuantity,item.itemUnitId, payload.createdBy, payload.createdTime or datetime.now()))

        for usefor_id in payload.useForIds:
            cursor.execute("""INSERT INTO product_used_for_mapping (productId, productUsedForId, createdBy, createdTime)VALUES (%s,%s,%s,%s)""", 
                           (product_id,usefor_id,payload.createdBy,payload.createdTime or datetime.now()))
        
        conn.commit()
        return {"status": "success", "statusCode": 200,"message": "Product successfully created","data": {"product_id": product_id}}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "failed","statusCode": 500,"message": "Error while creating product: " + str(e), "data": None}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_products_list():
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(""" SELECT p.id,p.productName,mdu.value AS units,p.make,p.model,p.productCode,DATE_FORMAT(p.publishedDate, '%b %d, %Y') AS publishedDate, GROUP_CONCAT(DISTINCT mdp.value ORDER BY mdp.value SEPARATOR ', ') AS useFor FROM products p LEFT JOIN product_used_for_mapping pum ON pum.productId = p.id  AND pum.active = 'T' LEFT JOIN metadata.metadata_details mdp ON mdp.key_id = pum.productUsedForId AND mdp.metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = 'Inv_productStatus') LEFT JOIN metadata.metadata_details mdu ON mdu.key_id = p.units AND mdu.metadata_types_id = (SELECT id FROM metadata.metadata_master WHERE type = 'Inv_Units') WHERE p.active = 'T' and pum.active = "T" GROUP BY  p.id, p.productName, p.units, mdu.value, p.make, p.model, p.productCode, p.publishedDate ORDER BY p.id DESC;""")
        products = cursor.fetchall()
        if not products: return {"status": "Failed","statusCode": 404,"message": "No products found","data": []}
        return {"status": "success","statusCode": 200,"message": "Products retrieved successfully","data": products}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed","statusCode": 500,"message": f"Error while retrieving products: {str(e)}","data": []}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_product_details(product_id: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""SELECT p.id,p.productName,p.productCode, md.value as units, p.make, p.model, DATE_FORMAT(p.publishedDate, '%b %d, %Y') AS publishedDate, (SELECT JSON_ARRAYAGG( md.value) FROM product_used_for_mapping pum LEFT JOIN (select key_id,value from metadata.metadata_details where metadata_types_id = (select id from metadata.metadata_master where type = "Inv_UsedFor")) md ON md.key_id = pum.productUsedForId WHERE pum.productId = p.id AND pum.active='T') AS useFor, (SELECT JSON_ARRAYAGG(JSON_OBJECT('itemId',i.id,'itemName', i.itemName,'quantity', pi.itemsQuantity,'units', md.value,'make',i.make,'model',i.model)) FROM product_items pi LEFT JOIN items i ON i.id = pi.itemsId LEFT JOIN (select key_id,value from metadata.metadata_details where metadata_types_id = (select id from metadata.metadata_master where type = "Inv_Units")) md on md.key_id = pi.itemsUnits  WHERE pi.productId = p.id) AS itemsList FROM products p  LEFT JOIN (select key_id,value from metadata.metadata_details where metadata_types_id = (select id from metadata.metadata_master where type = "Inv_Units")) md On p.units = md.key_id WHERE p.id = %s AND p.active='T' ;""", (product_id,))
        product = cursor.fetchone()
        if not product: return {"status": "Failed", "statusCode": 404, "message": "Product not found", "data": None}

        product["useFor"] = ", ".join(json.loads(product["useFor"])) if product["useFor"] else ""
        product["itemsList"] = json.loads(product["itemsList"]) if product["itemsList"] else []
        return {"status": "success","statusCode": 200,"message": "Product details fetched successfully","data": product}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed","statusCode": 500,"message": f"Error while retrieving product details: {str(e)}","data": None}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_available_items(itemId: int = None, storeId: int = None):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        query = """SELECT pi.id, pi.itemId, i.itemName, i.make, i.model, pi.serialNumber, pi.barcode, 1 AS quantity, i.units AS unitId, mdu.value AS units, pi.unitPrice, pi.gstPercent, (pi.unitPrice * ((pi.gstPercent/100) + 1)) AS total_value FROM purchase_items pi JOIN items i ON i.id = pi.itemId JOIN purchase_invoices pinv ON pinv.id = pi.purchaseId JOIN ( SELECT key_id,value FROM metadata.metadata_details WHERE metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = 'Inv_Units' ) ) mdu ON mdu.key_id = i.units WHERE pi.status = 'DELIVERED' AND (pi.serialNumber IS NOT NULL OR pi.barcode IS NOT NULL) """
        params = []
        if itemId:
            query += " AND pi.itemId = %s"
            params.append(itemId)
        if storeId:
            query += " AND pinv.purchaseToId = %s"
            params.append(storeId)

        query += """ UNION ALL SELECT MIN(pi.id) AS id, pi.itemId, MAX(i.itemName) AS itemName, MAX(i.make) AS make, MAX(i.model) AS model, NULL AS serialNumber, NULL AS barcode, COUNT(*) AS quantity, MAX(i.units) AS unitId, MAX(mdu.value) AS units, pi.unitPrice, pi.gstPercent, (pi.unitPrice * ((pi.gstPercent/100) + 1)) AS total_value FROM purchase_items pi JOIN items i ON i.id = pi.itemId JOIN purchase_invoices pinv ON pinv.id = pi.purchaseId JOIN ( SELECT key_id,value FROM metadata.metadata_details WHERE metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = 'Inv_Units' ) ) mdu ON mdu.key_id = i.units WHERE pi.status = 'DELIVERED' AND pi.serialNumber IS NULL AND pi.barcode IS NULL """
        if itemId:
            query += " AND pi.itemId = %s"
            params.append(itemId)
        if storeId:
            query += " AND pinv.purchaseToId = %s"
            params.append(storeId)
        query += " GROUP BY pi.itemId, pi.unitPrice, pi.gstPercent "

        cursor.execute(query, tuple(params))
        available_items = cursor.fetchall()
        if not available_items: return {"status": "Failed", "statusCode": 404, "message": "No available items found", "data": None}
        return {"status": "Success", "statusCode": 200, "message": "Available items fetched successfully", "data": available_items}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"Error while fetching available items: {str(e)}", "data": None}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_cost_of_items(productItemId: List[int]):
    conn = None
    cursor = None
    try:
        if not productItemId: return {"status": "Failed", "statusCode": 400,"message": "productItemId list cannot be empty","data": None }
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        placeholders = ",".join(["%s"] * len(productItemId))
        cursor.execute(f"SELECT SUM(unitPrice) AS totalCost FROM purchase_items WHERE id IN ({placeholders})", tuple(productItemId))
        return {"status": "success", "statusCode": 200,"message": "Cost of items fetched successfully", "data": cursor.fetchone() }
    except Exception as e:
        if conn: conn.rollback()
        return { "status": "Failed",  "statusCode": 500,"message": f"Error while fetching cost of items: {str(e)}", "data": None }
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

async def add_new_product(data_str: str, file):
    conn = None
    cursor = None
    try:
        data = CreateProductModel(**json.loads(data_str))
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        createdTime = data.createdTime or datetime.now()

        cursor.execute("SELECT id FROM product_details WHERE serialNumber=%s", (data.serialNumber,))
        if cursor.fetchone(): return {"status": "Failed", "statusCode": 400, "message": "Serial already exists"}

        cursor.execute("SELECT id FROM product_details WHERE barCode=%s", (data.barCode,))
        if cursor.fetchone(): return {"status": "Failed", "statusCode": 400, "message": "Barcode already exists"}

        cursor.execute("SELECT productName FROM products WHERE id = %s", (data.productId,))
        product_row = cursor.fetchone()
        if not product_row: return {"status": "Failed", "statusCode": 404, "message": "Product not found"}
        product_name = product_row["productName"].replace(" ", "")

        productImage = None
        if file:
            asset_name = f"{data.productId}_{product_name}_{file.filename.split('.')[0]}"
            upload_response = await upload_file_to_s3_handler(assetFile=file, requestName="inventory", assetName=asset_name)
            result = json.loads(upload_response.body) if hasattr(upload_response, "body") else upload_response
            if result.get("statusCode", 200) != 200: return {"status": "Failed", "statusCode": result["statusCode"], "message": f"Error uploading file to S3: {result['message']}"}
            productImage = result.get("assetName")

        total_cost = 0
        purchase_items_to_use = []

        for item in data.itemsUsed:
            if item.purchaseItemIds:
                placeholders = ",".join(["%s"] * len(item.purchaseItemIds))
                cursor.execute(f"SELECT id,itemId,status,unitPrice FROM purchase_items WHERE id IN ({placeholders})", tuple(item.purchaseItemIds))
                rows = cursor.fetchall()
                if len(rows) != len(item.purchaseItemIds): return {"status": "Failed", "statusCode": 404, "message": "Some purchase items not found"}
                for r in rows:
                    if r["status"] != "DELIVERED": return {"status": "Failed", "statusCode": 404, "message": f"Purchase item {r['id']} not available"}
                    if r["itemId"] != item.itemId: return {"status": "Failed", "statusCode": 400, "message": f"Item mismatch for purchase item {r['id']}"}
                    total_cost += r["unitPrice"]
                    purchase_items_to_use.append(r)
            else:
                cursor.execute("SELECT id,itemId,status,unitPrice FROM purchase_items WHERE itemId=%s AND status='DELIVERED' ORDER BY id ASC LIMIT %s", (item.itemId, item.quantity))
                rows = cursor.fetchall()
                if len(rows) < item.quantity: return {"status": "Failed", "statusCode": 404, "message": f"Not enough stock for item {item.itemId}"}
                for r in rows:
                    total_cost += r["unitPrice"]
                    purchase_items_to_use.append(r)

        cursor.execute("""INSERT INTO product_details (productId,serialNumber,barCode,quantity,totalCost,currentLocationId, productImage,createdBy,createdTime,remarks) VALUES (%s,%s,%s,1,%s,%s,%s,%s,%s,%s)""",
            (data.productId, data.serialNumber, data.barCode, total_cost, data.currentLocationId, productImage, data.createdBy, createdTime, data.remarks))
        product_details_id = cursor.lastrowid

        for r in purchase_items_to_use:
            cursor.execute("INSERT INTO product_used_items (productDetailsId,itemId,purchaseItemId,cost,createdBy,createdTime) VALUES (%s,%s,%s,%s,%s,%s)", (product_details_id, r["itemId"], r["id"], r["unitPrice"], data.createdBy, createdTime))
            cursor.execute("INSERT INTO stock_ledger (itemId,stockHolderId,action,actionId,actionItemId, qtyIn,qtyOut,createdBy,createdTime) VALUES (%s,%s,'USED',%s,%s,0,1,%s,%s)", (r["itemId"], data.currentLocationId, product_details_id, r["id"], data.createdBy, createdTime))
            cursor.execute("UPDATE purchase_items SET status='USED', modifiedBy=%s, modifiedTime=%s WHERE id=%s", (data.createdBy, createdTime, r["id"]))

        conn.commit()
        return {"status": "Success", "statusCode": 200, "message": "Product created successfully", "data": {"productDetailsId": product_details_id, "totalCost": total_cost}}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"error while creating product {str(e)}", "data": None}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_all_products_list(pageNo: int, pageSize: int, search: str, statusId: int, locationId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        offset = (pageNo - 1) * pageSize
        where_clauses, params = [], []

        if search:
            where_clauses.append("(p.productName LIKE %s OR pd.serialNumber LIKE %s OR pd.barCode LIKE %s)")
            params.extend([f"%{search}%"] * 3)
        if locationId:
            where_clauses.append("pd.currentLocationId = %s")
            params.append(locationId)
        if statusId:
            where_clauses.append("pd.statusId = %s")
            params.append(statusId)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        cursor.execute(f"SELECT COUNT(*) AS totalRecords FROM product_details pd LEFT JOIN products p ON p.id = pd.productId {where_sql}", tuple(params))
        totalRecords = cursor.fetchone()["totalRecords"]

        cursor.execute(f"""SELECT pd.id, p.productName, p.make,p.model, pd.serialNumber, pd.barCode, pd.quantity, DATE_FORMAT(pd.publishedDate, '%b %d, %Y') as publishedDate, CONCAT(l.entityType, ' - ', l.name , ' - ', l.country) as currentLocation, mdps.value as status, CASE WHEN pd.productImage IS NOT NULL THEN CONCAT( 'https://usstaging.ivisecurity.com/common/downloadFile_1_0?requestName=inventory&assetName=', pd.productImage ) ELSE NULL END AS productImage FROM product_details pd LEFT JOIN products p ON p.id = pd.productId LEFT JOIN (SELECT * FROM (SELECT id, entityType, name, country, status FROM locations UNION ALL SELECT siteId, 'site', siteName, country, status FROM vip_sites_management.sites) AS merged WHERE status IN ('T', 'Active')) l ON l.id = pd.currentLocationId LEFT JOIN ( SELECT key_id,value FROM metadata.metadata_details WHERE metadata_types_id = ( SELECT id FROM metadata.metadata_master WHERE type = "Inv_productStatus")) mdps ON mdps.key_id = pd.statusId {where_sql} ORDER BY pd.id DESC LIMIT %s OFFSET %s """, tuple(params + [pageSize, offset]))
        rows = cursor.fetchall()

        if not rows: return {"status": "Failed", "statusCode": 404,  "message": "No products found",  "data": [], "pagination": { "pageNo": pageNo, "pageSize": pageSize, "totalRecords": totalRecords,"totalPages": (totalRecords + pageSize - 1) // pageSize } }
        return { "status": "Success", "statusCode": 200,"message": "Products fetched successfully", "data": rows, "pagination": { "pageNo": pageNo,"pageSize": pageSize, "totalRecords": totalRecords, "totalPages": (totalRecords + pageSize - 1) // pageSize}}
    except Exception as e:
        if conn: conn.rollback()
        return { "status": "Failed","statusCode": 500, "message": f"error while getting products {str(e)}","data": [] }
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_product_all_details(productDetailId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)

        locations_sql = "SELECT id, entityType, name FROM locations UNION ALL SELECT siteId AS id, 'site' AS entityType, siteName AS name FROM vip_sites_management.sites"
        units_sql = "SELECT mdd.key_id, mdd.value FROM metadata.metadata_details mdd JOIN metadata.metadata_master mdm ON mdm.id = mdd.metadata_types_id WHERE mdm.type='Inv_Units'"
        status_sql = "SELECT mdd.key_id, mdd.value FROM metadata.metadata_details mdd JOIN metadata.metadata_master mdm ON mdm.id = mdd.metadata_types_id WHERE mdm.type='Inv_productStatus'"

        cursor.execute(f"""
        SELECT pd.id, p.productName, p.make, p.model, pd.serialNumber, pd.barCode, pd.publishedDate, pd.createdTime, u.value AS unit, s.value AS status, uf.usedFor, loc.name AS locationName, loc.entityType, pd.currentLocationId
        FROM product_details pd JOIN products p ON p.id = pd.productId LEFT JOIN ({units_sql}) u ON u.key_id = p.units LEFT JOIN ({status_sql}) s ON s.key_id = pd.statusId LEFT JOIN ( SELECT pum.productId, GROUP_CONCAT(mdd.value SEPARATOR ', ') AS usedFor FROM product_used_for_mapping pum JOIN metadata.metadata_details mdd ON mdd.key_id = pum.productUsedForId JOIN metadata.metadata_master mdm ON mdm.id = mdd.metadata_types_id WHERE mdm.type = 'Inv_productStatus' GROUP BY pum.productId ) uf ON uf.productId = p.id LEFT JOIN ({locations_sql}) loc ON loc.id = pd.currentLocationId WHERE pd.id = %s
        """,(productDetailId,))
        product = cursor.fetchone()
        if not product: return {"status": "Failed", "statusCode": 404, "message": "Product not found"}

        cursor.execute(f"""
        SELECT i.itemName, i.make, i.model, u.value AS units, pui.cost AS baseCost, pi.gstPercent, ROUND(pui.cost + (pui.cost * pi.gstPercent / 100),2) AS cost, GROUP_CONCAT(DISTINCT ipl.purchaseLink SEPARATOR ', ') AS purchaseLinks, i.itemImage
        FROM product_used_items pui JOIN items i ON i.id = pui.itemId JOIN purchase_items pi ON pi.id = pui.purchaseItemId LEFT JOIN item_purchase_links ipl ON ipl.itemId = i.id AND ipl.active = 'T' LEFT JOIN ({units_sql}) u ON u.key_id = i.units WHERE pui.productDetailsId = %s GROUP BY pui.id, i.itemName, i.make, i.model, u.value, pui.cost, pi.gstPercent, i.itemImage
        """,(productDetailId,))
        hardware = cursor.fetchall()

        base_url = "https://usstaging.ivisecurity.com/common/downloadFile_1_0?requestName=inventory&assetName="
        for row in hardware: row["itemImage"] = base_url + row["itemImage"] if row["itemImage"] else None
        manufacturingCost = sum(row["cost"] for row in hardware if row["cost"])

        cursor.execute(f"""
        SELECT pd.createdTime AS eventDate, fromLoc.name AS fromLocation, toLoc.name AS toLocation, 'New' AS action FROM product_details pd LEFT JOIN ({locations_sql}) toLoc ON toLoc.id = pd.currentLocationId LEFT JOIN purchase_items pi ON pi.id = ( SELECT purchaseItemId FROM product_used_items WHERE productDetailsId = pd.id LIMIT 1 ) LEFT JOIN purchase_invoices inv ON inv.id = pi.purchaseId LEFT JOIN ({locations_sql}) fromLoc ON fromLoc.id = inv.purchaseFromId WHERE pd.id = %s
        UNION ALL
        SELECT i.issueDate AS eventDate, fromLoc.name, toLoc.name, CONCAT( CASE WHEN ip.status='SALE' THEN 'Sale' WHEN ip.status='LEASE' THEN 'Lease' ELSE ip.status END, ' ', CASE WHEN i.status='DELIVERED' THEN 'Delivered' ELSE 'Issued' END ) AS action FROM issue_products ip JOIN issued i ON i.id = ip.issueId LEFT JOIN ({locations_sql}) fromLoc ON fromLoc.id = i.issuedFromId LEFT JOIN ({locations_sql}) toLoc ON toLoc.id = i.issuedToId WHERE ip.productDetailsId = %s ORDER BY eventDate
        """,(productDetailId,productDetailId))

        progress = [{"date": r["eventDate"].strftime("%d/%m/%Y") if r["eventDate"] else None, "from": r["fromLocation"], "to": r["toLocation"], "action": r["action"]} for r in cursor.fetchall()]

        return {"status": "Success", "statusCode": 200, "data": { "header": {"title": product["productName"], "subtitle": f'{product["make"]} - {product["model"]}'}, "productDetails": {"serialNumber": product["serialNumber"], "barcode": product["barCode"], "unit": product["unit"], "status": product["status"], "usedFor": product["usedFor"], "location": product["locationName"], "publishedDate": product["publishedDate"]}, "hardware": hardware, "manufacturingCost": manufacturingCost, "progress": progress}}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"Error getting product details: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def update_product(data: UpdateProductModel):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        modifiedTime = datetime.now()

        cursor.execute("SELECT id FROM product_details WHERE id = %s", (data.productDetailId,))
        if not cursor.fetchone(): return {"status":"Failed", "statusCode":404, "message":"Product not found"}

        cursor.execute("UPDATE product_details SET currentLocationId = COALESCE(%s,currentLocationId), statusId = COALESCE(%s,statusId), remarks = COALESCE(%s,remarks), publishedDate = COALESCE(%s,publishedDate), modifiedBy = %s, modifiedTime = %s WHERE id = %s",
            (data.currentLocationId, data.statusId, data.remarks, data.publishedDate, data.modifiedBy, modifiedTime, data.productDetailId))

        if data.usedFor:
            cursor.execute("DELETE FROM product_used_for_mapping WHERE productId = (SELECT productId FROM product_details WHERE id = %s)",(data.productDetailId,))
            cursor.execute("SELECT productId FROM product_details WHERE id = %s",(data.productDetailId,))
            productId = cursor.fetchone()["productId"]
            for u in data.usedFor:
                cursor.execute("INSERT INTO product_used_for_mapping (productId,productUsedForId,active,createdBy,createdTime) VALUES (%s,%s,'T',%s,%s)",(productId, u, data.modifiedBy, modifiedTime))

        conn.commit()
        return {"status":"Success", "statusCode":200, "message":"Product updated successfully"}
    except Exception as e:
        if conn: conn.rollback()
        return {"status":"Failed", "statusCode":500, "message":f"Error updating product: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()