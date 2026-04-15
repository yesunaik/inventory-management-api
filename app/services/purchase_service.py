import json
from typing import Optional, List
from datetime import datetime, timedelta, date
from app.db.db import dbconn_inventory
from app.utils.functions import upload_file_to_s3_handler
from app.schemas.inventory_schemas import PurchaseCreateModel, PurchaseUpdateModel

def get_purchase_sources(country: Optional[str], entityType: Optional[str]):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)

        query = """SELECT * FROM (SELECT id, entityType, name, country, status FROM locations UNION ALL SELECT siteId, 'site', siteName, country, status FROM vip_sites_management.sites ) AS merged WHERE status IN ('T', 'Active') """
        params = []

        if country:
            query += " AND country = %s"
            params.append(country)

        if entityType:
            query += " AND entityType = %s"
            params.append(entityType)

        cursor.execute(query, tuple(params))
        data = cursor.fetchall()
        sources  = []
        for row in data:
            sources.append({
                "sourceId": row["id"],
                "sourceName": row["name"].capitalize(),
                "sourceType": row["entityType"].capitalize(),
                "source_country" : row["country"].capitalize(),
                "source_status" : row["status"]
            })
        return {"status": "Success", "statusCode": 200, "message": "Data retrieved successfully", "data": sources}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"Error while retrieving data: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

async def create_purchase_invoice(purchase: str, invoiceFiles: List, otherFiles: List):
    conn = None
    cursor = None
    try:
        purchase_data = PurchaseCreateModel(**json.loads(purchase))
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)

        if not purchase_data.items: return {"status": "Failed", "statusCode": 400, "message": "Purchase items cannot be empty", "data": None}
        status = "PREORDER" if purchase_data.invoiceType.upper() == "PREORDER" else "DELIVERED"

        item_ids = [item.itemId for item in purchase_data.items]
        format_strings = ','.join(['%s'] * len(item_ids))
        cursor.execute(f"SELECT id, serialNumberFlag, barcodeFlag FROM items WHERE id IN ({format_strings})", tuple(item_ids))
        rows = cursor.fetchall()
        item_flags_map = {row["id"]: row for row in rows}

        missing_items = [item.itemId for item in purchase_data.items if item.itemId not in item_flags_map]
        if missing_items: return {"status": "Failed", "statusCode": 404, "message": f"Items not found: {missing_items}", "data": None}

        all_serials, all_barcodes = [], []
        if status == "DELIVERED":
            for item in purchase_data.items:
                item_flag = item_flags_map[item.itemId]
                serial_flag = item_flag["serialNumberFlag"]
                barcode_flag = item_flag["barcodeFlag"]

                if serial_flag == "T":
                    if not item.serialNumbers: return {"status": "Failed", "statusCode": 400, "message": f"Serial numbers required for item {item.itemId}", "data": None}
                    if len(item.serialNumbers) != item.quantity: return {"status": "Failed", "statusCode": 400, "message": f"Serial numbers must match quantity for item {item.itemId}", "data": None}
                    all_serials.extend(item.serialNumbers)

                if barcode_flag == "T":
                    if not item.barcodes: return {"status": "Failed", "statusCode": 400, "message": f"Barcodes required for item {item.itemId}", "data": None}
                    if len(item.barcodes) != item.quantity: return {"status": "Failed", "statusCode": 400, "message": f"Barcode count must match quantity for item {item.itemId}", "data": None}
                    all_barcodes.extend(item.barcodes)

        if len(all_serials) != len(set(all_serials)): return {"status": "Failed", "statusCode": 400, "message": "Duplicate serial numbers in request", "data": None}
        if len(all_barcodes) != len(set(all_barcodes)): return {"status": "Failed", "statusCode": 400, "message": "Duplicate barcodes in request", "data": None}

        if all_serials:
            format_strings = ','.join(['%s'] * len(all_serials))
            cursor.execute(f"SELECT serialNumber FROM purchase_items WHERE serialNumber IN ({format_strings})", tuple(all_serials))
            if cursor.fetchall(): return {"status": "Failed", "statusCode": 409, "message": "Serial already exists", "data": None}

        if all_barcodes:
            format_strings = ','.join(['%s'] * len(all_barcodes))
            cursor.execute(f"SELECT barcode FROM purchase_items WHERE barcode IN ({format_strings})", tuple(all_barcodes))
            if cursor.fetchall(): return {"status": "Failed", "statusCode": 409, "message": "Barcode already exists", "data": None}

        if purchase_data.invoiceNumber:
            cursor.execute("SELECT id FROM purchase_invoices WHERE invoiceNumber=%s", (purchase_data.invoiceNumber.strip(),))
            if cursor.fetchone(): return {"status": "Failed", "statusCode": 409, "message": "Invoice number already exists", "data": None}

        totalAmount = sum((item.quantity * item.unitPrice) + ((item.quantity * item.unitPrice) * item.gstPercent / 100) for item in purchase_data.items)
        totalItems = len(purchase_data.items)

        cursor.execute("""
        INSERT INTO purchase_invoices (invoiceNumber,purchaseFromId,purchaseToId,purchaseType,invoiceDate, totalItems,totalAmount,status,createdBy,createdTime) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (purchase_data.invoiceNumber.strip() if purchase_data.invoiceNumber else None, purchase_data.purchaseFromId, purchase_data.purchaseToId, purchase_data.purchaseType.strip(), purchase_data.invoiceDate, totalItems, totalAmount, status, purchase_data.createdBy, purchase_data.createdTime or datetime.now()))
        purchase_id = cursor.lastrowid

        for item in purchase_data.items:
            item_flag = item_flags_map[item.itemId]
            serial_flag = item_flag["serialNumberFlag"]
            barcode_flag = item_flag["barcodeFlag"]

            for i in range(item.quantity):
                serial = item.serialNumbers[i] if serial_flag == "T" and item.serialNumbers else None
                barcode = item.barcodes[i] if barcode_flag == "T" and item.barcodes else None

                cursor.execute("""
                INSERT INTO purchase_items (purchaseId,itemId,serialNumber,barcode,unitPrice,gstPercent,status,createdBy,createdTime) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (purchase_id, item.itemId, serial, barcode, item.unitPrice, item.gstPercent, status, purchase_data.createdBy, purchase_data.createdTime or datetime.now()))
                purchase_item_id = cursor.lastrowid

                if status == "DELIVERED":
                    cursor.execute("""
                    INSERT INTO stock_ledger (itemId,stockHolderId,action,actionId,actionItemId,qtyIn,qtyOut,createdBy,createdTime) VALUES (%s,%s,'PURCHASE',%s,%s,1,0,%s,%s)""",
                    (item.itemId, purchase_data.purchaseToId, purchase_id, purchase_item_id, purchase_data.createdBy, purchase_data.createdTime or datetime.now()))

        # S3 INVOICE FILES
        for file in invoiceFiles or []:
            asset_name = f"purchase_{purchase_id}_{file.filename.split('.')[0]}"
            upload_response = await upload_file_to_s3_handler(assetFile=file, requestName="inventory", assetName=asset_name)
            result = json.loads(upload_response.body) if hasattr(upload_response, "body") else upload_response
            if result.get("statusCode", 200) != 200: return {"status": "Failed", "statusCode": result["statusCode"], "message": f"Error uploading invoice file: {result.get('message')}"}
            
            cursor.execute("""INSERT INTO purchase_files (purchaseId, fileType, originalFileName, storedFileName, fileSize, createdBy, createdTime) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (purchase_id, "INVOICE", file.filename, result.get("assetName"), file.size if hasattr(file, 'size') else 0, purchase_data.createdBy, purchase_data.createdTime or datetime.now()))

        # S3 OTHER FILES
        for file in otherFiles or []:
            asset_name = f"purchase_{purchase_id}_{file.filename.split('.')[0]}"
            upload_response = await upload_file_to_s3_handler(assetFile=file, requestName="inventory", assetName=asset_name)
            result = json.loads(upload_response.body) if hasattr(upload_response, "body") else upload_response
            if result.get("statusCode", 200) != 200: return {"status": "Failed", "statusCode": result["statusCode"], "message": f"Error uploading other file: {result.get('message')}"}
            
            cursor.execute("""INSERT INTO purchase_files (purchaseId, fileType, originalFileName, storedFileName, fileSize, createdBy, createdTime) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (purchase_id, "OTHER", file.filename, result.get("assetName"), file.size if hasattr(file, 'size') else 0, purchase_data.createdBy, purchase_data.createdTime or datetime.now()))

        conn.commit()
        return {"status": "Success", "statusCode": 200, "message": "Purchase created successfully", "data": {"purchase_id": purchase_id, "totalItems": totalItems, "totalAmount": totalAmount}}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"error while creating purchase: {str(e)}", "data": None}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_purchase_list(startDate: str, endDate: str, pageNo: int, pageSize: int, storeId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        today = datetime.now()
        endDate = endDate or today
        startDate = startDate or (endDate - timedelta(days=30))
        offset = (pageNo - 1) * pageSize

        where_conditions = ["p.invoiceDate BETWEEN %s AND %s"]
        params = [startDate, endDate]
        if storeId:
            where_conditions.append("p.purchaseToId = %s")
            params.append(storeId)

        where_clause = "WHERE " + " AND ".join(where_conditions)
        cursor.execute(f"SELECT COUNT(DISTINCT p.id) AS totalRecords FROM purchase_invoices p JOIN purchase_items pi ON p.id = pi.purchaseId {where_clause}", params)
        totalRecords = cursor.fetchone()["totalRecords"]

        data_query = f"""
            SELECT p.id, p.invoiceDate, p.invoiceNumber, lf.name AS purchaseFrom, lt.name AS deliveredTo, p.purchaseType, p.totalItems, COUNT(pi.id) AS totalItemsQuantity, SUM(CASE WHEN pi.status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_count, SUM(CASE WHEN pi.status = 'ISSUED' THEN 1 ELSE 0 END) AS issued_count, SUM(CASE WHEN pi.status = 'USED' THEN 1 ELSE 0 END) AS used_count, SUM(CASE WHEN pi.status = 'PREORDER' THEN 1 ELSE 0 END) AS preorder_count, SUM(CASE WHEN pi.status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_count
            FROM purchase_invoices p JOIN purchase_items pi ON p.id = pi.purchaseId
            LEFT JOIN (SELECT id, entityType, name, country, status FROM locations WHERE status IN ('T','Active') UNION ALL SELECT siteId AS id, 'site' AS entityType, siteName AS name, country, status FROM vip_sites_management.sites WHERE status IN ('T','Active')) lf ON lf.id = p.purchaseFromId
            LEFT JOIN (SELECT id, entityType, name, country, status FROM locations WHERE status IN ('T','Active') UNION ALL SELECT siteId AS id, 'site' AS entityType, siteName AS name, country, status FROM vip_sites_management.sites WHERE status IN ('T','Active')) lt ON lt.id = p.purchaseToId
            {where_clause} GROUP BY p.id, p.invoiceDate, p.invoiceNumber, lf.name, lt.name, p.purchaseType ORDER BY p.id DESC LIMIT %s OFFSET %s
        """
        cursor.execute(data_query, params + [pageSize, offset])
        rows = cursor.fetchall()
        result = []

        for row in rows:
            total = row["totalItemsQuantity"]
            delivered, issued, used, returned, preorder = row["delivered_count"] or 0, row["issued_count"] or 0, row["used_count"] or 0, row["returned_count"] or 0, row["preorder_count"] or 0

            if (delivered + issued + used) == total: status, color = "DELIVERED", "#53BF8B"
            elif returned == total: status, color = "RETURNED", "#ED3237"
            elif preorder == total: status, color = "PREORDER", "#000000"
            else: status, color = "DELIVERED", "#FFC400"

            result.append({"purchase_id": row["id"], "invoiceNumber": row["invoiceNumber"], "purchaseFrom": row["purchaseFrom"], "deliveredToName": row["deliveredTo"], "invoiceDate": row["invoiceDate"], "purchaseType": row["purchaseType"], "totalItems": row["totalItems"], "status": status, "statusColor": color})

        return {"status": "Success", "statusCode": 200, "dateRangeUsed": {"startDate": startDate, "endDate": endDate}, "data": result, "pagination": {"pageNo": pageNo, "pageSize": pageSize, "totalRecords": totalRecords, "totalPages": (totalRecords + pageSize - 1) // pageSize}}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"error while fetching purchase list: {str(e)}", "data": []}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_purchase_details(purchaseId: int):
    def calculate_status(delivered, issued, returned, preorder, total):
        if (delivered + issued) == total: return "DELIVERED", "#53BF8B"
        elif returned == total: return "RETURNED", "#ED3237"
        elif preorder == total: return "PREORDER", "#000000"
        else: return "DELIVERED", "#FFC400"

    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
        SELECT p.id, p.invoiceNumber, p.invoiceDate, lf.name AS purchaseFrom, lt.name AS deliveredTo, p.purchaseType, p.totalAmount
        FROM purchase_invoices p LEFT JOIN (SELECT * FROM (SELECT id, entityType, name, country, status FROM locations UNION ALL SELECT siteId, 'site', siteName, country, status FROM vip_sites_management.sites) merged WHERE status IN ('T','Active')) lt ON p.purchaseToId = lt.id LEFT JOIN (SELECT * FROM (SELECT id, entityType, name, country, status FROM locations UNION ALL SELECT siteId, 'site', siteName, country, status FROM vip_sites_management.sites) merged WHERE status IN ('T','Active')) lf ON p.purchaseFromId = lf.id WHERE p.id=%s
        """,(purchaseId,))
        purchase = cursor.fetchone()
        if not purchase: return {"status":"Failed", "statusCode":404, "message":"Purchase not found", "data":None}

        cursor.execute("""
        SELECT pi.id, pi.itemId, i.itemName, i.itemCode, i.make, i.model, i.serialNumberFlag, i.barcodeFlag, pi.serialNumber, pi.barcode, pi.unitPrice, pi.gstPercent, pi.status, pi.returnReason FROM purchase_items pi JOIN items i ON pi.itemId=i.id WHERE pi.purchaseId=%s ORDER BY pi.itemId,pi.id
        """,(purchaseId,))
        rows = cursor.fetchall()

        items_grouped = {}
        for row in rows:
            itemId = row["itemId"]
            if itemId not in items_grouped:
                items_grouped[itemId] = {"itemId":itemId, "itemName":row["itemName"], "itemCode":row["itemCode"], "make":row["make"], "model":row["model"], "serialNumberFlag":row["serialNumberFlag"], "barcodeFlag":row["barcodeFlag"], "unitPrice":row["unitPrice"], "gstPercent":row["gstPercent"], "count":0, "delivered":0, "issued":0, "returned":0, "preorder":0, "serials":[]}
            
            items_grouped[itemId]["count"]+=1
            if row["status"]=="DELIVERED": items_grouped[itemId]["delivered"]+=1
            elif row["status"]=="ISSUED": items_grouped[itemId]["issued"]+=1
            elif row["status"]=="RETURNED": items_grouped[itemId]["returned"]+=1
            elif row["status"]=="PREORDER": items_grouped[itemId]["preorder"]+=1

            serial_obj = {"purchaseItemId":row["id"], "serialNumber":row["serialNumber"], "barcode":row["barcode"], "status":row["status"]}
            if row["status"]=="RETURNED": serial_obj["returnReason"]=row["returnReason"]
            items_grouped[itemId]["serials"].append(serial_obj)

        final_items = []
        total_items_count, total_delivered, total_returned, total_preorder, total_issued = 0, 0, 0, 0, 0
        for item in items_grouped.values():
            total_price = item["unitPrice"]+(item["unitPrice"]*item["gstPercent"]/100)
            item_status, item_color = calculate_status(item["delivered"], item["issued"], item["returned"], item["preorder"], item["count"])
            final_items.append({"itemId":item["itemId"], "itemName":item["itemName"], "itemCode":item["itemCode"], "make":item["make"], "model":item["model"], "serialNumberFlag":item["serialNumberFlag"], "barcodeFlag":item["barcodeFlag"], "count":item["count"], "unitPrice":item["unitPrice"], "gstPercent":item["gstPercent"], "totalPricePerUnit":round(total_price,2), "status":item_status, "statusColor":item_color, "serialDetails":item["serials"]})
            total_items_count+=item["count"]
            total_delivered+=item["delivered"]
            total_returned+=item["returned"]
            total_preorder+=item["preorder"]
            total_issued+=item["issued"]

        purchase_status, purchase_color = calculate_status(total_delivered, total_issued, total_returned, total_preorder, total_items_count)
        purchase["status"] = purchase_status
        purchase["statusColor"] = purchase_color

        cursor.execute("""
        SELECT id, fileType, originalFileName, storedFileName, CASE WHEN storedFileName IS NOT NULL AND storedFileName != '' THEN CONCAT( 'https://usstaging.ivisecurity.com/common/downloadFile_1_0?requestName=inventory&assetName=', storedFileName ) ELSE NULL END AS fileUrl FROM purchase_files WHERE purchaseId=%s
        """, (purchaseId,))
        files = cursor.fetchall()

        return {"status": "Success", "statusCode": 200, "message": "Purchase details retrieved successfully", "data": {"purchase": purchase, "items": final_items, "files": files}}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"Error while retrieving purchase details: {str(e)}", "data": None}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def update_purchase(data: PurchaseUpdateModel):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        purchaseId = data.purchaseId
        modifiedBy = data.modifiedBy
        modifiedTime = data.modifiedTime or datetime.now()

        cursor.execute("SELECT status,purchaseToId FROM purchase_invoices WHERE id=%s", (purchaseId,))
        purchase = cursor.fetchone()
        if not purchase: return {"status":"Failed","statusCode":404,"message":"Purchase not found"}
        purchaseToId = purchase["purchaseToId"]

        cursor.execute("SELECT id,serialNumberFlag,barcodeFlag FROM items")
        item_flags = {row["id"]:row for row in cursor.fetchall()}

        if data.invoiceNumber:
            cursor.execute("UPDATE purchase_invoices SET invoiceNumber=%s, modifiedBy=%s, modifiedTime=%s WHERE id=%s", (data.invoiceNumber,modifiedBy,modifiedTime,purchaseId))

        for item in data.items:
            cursor.execute("SELECT id,itemId,status FROM purchase_items WHERE id=%s AND purchaseId=%s", (item.purchaseItemId,purchaseId))
            row = cursor.fetchone()
            if not row: return {"status":"Failed", "statusCode":404, "message":f"Purchase item {item.purchaseItemId} not found"}

            itemId = row["itemId"]
            currentStatus = row["status"]
            flags = item_flags.get(itemId)
            serial_flag = flags["serialNumberFlag"]
            barcode_flag = flags["barcodeFlag"]
            serial = item.serialNumber
            barcode = item.barcode

            if item.status == "DELIVERED":
                if currentStatus != "PREORDER": return {"status":"Failed", "statusCode":400, "message":"Only PREORDER items can be delivered"}
                
                if serial_flag == "T":
                    if not serial: return {"status":"Failed", "statusCode":400, "message":f"Serial number required for item {itemId}"}
                    cursor.execute("SELECT id FROM purchase_items WHERE serialNumber=%s", (serial,))
                    if cursor.fetchone(): return {"status":"Failed", "statusCode":409, "message":f"Serial {serial} already exists"}
                else: serial = None

                if barcode_flag == "T":
                    if not barcode: return {"status":"Failed", "statusCode":400, "message":f"Barcode required for item {itemId}"}
                    cursor.execute("SELECT id FROM purchase_items WHERE barcode=%s", (barcode,))
                    if cursor.fetchone(): return {"status":"Failed", "statusCode":409, "message":f"Barcode {barcode} already exists"}
                else: barcode = None

                cursor.execute("UPDATE purchase_items SET serialNumber=%s, barcode=%s, status='DELIVERED', modifiedBy=%s, modifiedTime=%s WHERE id=%s", (serial,barcode,modifiedBy,modifiedTime,item.purchaseItemId))
                cursor.execute("INSERT INTO stock_ledger( itemId, stockHolderId, action, actionId, actionItemId, qtyIn, qtyOut, createdBy, createdTime, movementType ) VALUES(%s,%s,'PURCHASE',%s,%s,1,0,%s,%s,'INITIAL')", (itemId,purchaseToId,purchaseId,item.purchaseItemId,modifiedBy,modifiedTime))

            elif item.status == "RETURNED":
                if currentStatus != "DELIVERED": return {"status":"Failed", "statusCode":400, "message":"Only DELIVERED items can be returned"}
                if not item.returnReason: return {"status":"Failed", "statusCode":400, "message":"Return reason required"}

                cursor.execute("UPDATE purchase_items SET status='RETURNED', returnReason=%s, modifiedBy=%s, modifiedTime=%s WHERE id=%s", (item.returnReason,modifiedBy,modifiedTime,item.purchaseItemId))
                cursor.execute("UPDATE stock_ledger SET status='F', movementType='REVERSAL', modifiedBy=%s, modifiedTime=%s WHERE action='PURCHASE' AND actionId=%s AND actionItemId=%s AND status='T'", (modifiedBy,modifiedTime,purchaseId,item.purchaseItemId))

        cursor.execute("SELECT COUNT(*) total, SUM(status='DELIVERED') delivered, SUM(status='RETURNED') returned, SUM(status='PREORDER') preorder FROM purchase_items WHERE purchaseId=%s", (purchaseId,))
        s = cursor.fetchone()
        total = s["total"]
        delivered, returned, preorder = s["delivered"] or 0, s["returned"] or 0, s["preorder"] or 0

        if delivered == total: final_status = "DELIVERED"
        elif returned == total: final_status = "RETURNED"
        elif preorder == total: final_status = "PREORDER"
        else: final_status = "DELIVERED"

        cursor.execute("UPDATE purchase_invoices SET status=%s, modifiedBy=%s, modifiedTime=%s WHERE id=%s", (final_status,modifiedBy,modifiedTime,purchaseId))

        conn.commit()
        return {"status":"Success", "statusCode":200, "message":"Purchase updated successfully"}
    except Exception as e:
        if conn: conn.rollback()
        return {"status":"Failed", "statusCode":500, "message":str(e)}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()