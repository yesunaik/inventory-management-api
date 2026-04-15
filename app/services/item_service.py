import json
from typing import Optional, List
from datetime import datetime
from app.db.db import dbconn_inventory
from app.utils.functions import upload_file_to_s3_handler
from app.schemas.inventory_schemas import ItemSaveRequest, ItemUpdateRequest

def get_itemcode(itemName: str, nature: Optional[str], domain: Optional[str], partCode: Optional[str], made: Optional[str]):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        if nature and domain and partCode and made:
            base_code = f"{nature.strip()}-{domain.strip()}-{partCode.strip()}-{made.strip()}".upper()
        else:
            base_code = None

        if base_code:
            query = """SELECT COALESCE( MAX(CAST(SUBSTRING_INDEX(itemCode, '-', -1) AS UNSIGNED)),0) AS max_sequence FROM items WHERE itemCode LIKE %s"""
            cursor.execute(query, (f"{base_code}-%",))
            row = cursor.fetchone()
            max_seq = row["max_sequence"] or 0
            next_seq = max_seq + 1
            preview_code = f"{base_code}-{str(next_seq).zfill(5)}"
            return {"status": "success","statusCode": 200 ,"message": "Item code generated successfully", "itemCode": preview_code}
        else :
            query = """SELECT itemCode FROM items WHERE itemName = %s ORDER BY id DESC LIMIT 1"""
            cursor.execute(query, (itemName.strip(),))
            row = cursor.fetchone()
            if row and row["itemCode"]:
                stripped_code = int(row["itemCode"].split("-")[-1]) + 1
                new_code = f"{row['itemCode'].rsplit('-', 1)[0]}-{str(stripped_code).zfill(5)}"
                return {"status": "success","statusCode": 200 ,"message": "Item code generated successfully", "itemCode": new_code}
            else:
                return {"status": "failed","statusCode": 400 ,"message": "Base parameters required for new item code generation", "itemCode": None}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "failed","statusCode": 500,"message": str(e), "itemCode": None}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

async def add_new_item(item_data: str, file):
    conn = None
    cursor = None
    try:
        item = ItemSaveRequest(**json.loads(item_data))
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        createdTime = item.createdTime or datetime.now()

        cursor.execute("SELECT id FROM items WHERE itemCode=%s", (item.itemCode,))
        if cursor.fetchone(): return {"status": "Failed", "statusCode": 409, "message": "Item code already exists"}

        cursor.execute("SELECT id FROM items WHERE itemName=%s AND make=%s AND model=%s", (item.itemName, item.make, item.model))
        if cursor.fetchone(): return {"status": "Failed", "statusCode": 409, "message": "Item already exists"}

        units = item.units
        if units is None:
            cursor.execute("SELECT units FROM items WHERE itemName=%s LIMIT 1", (item.itemName,))
            row = cursor.fetchone()
            units = row["units"] if row else 2

        imagePath = None
        if file:
            asset_name = f"{item.itemCode}_{file.filename.split('.')[0]}"
            upload_response = await upload_file_to_s3_handler(assetFile=file, requestName="inventory", assetName=asset_name)
            result = json.loads(upload_response.body) if hasattr(upload_response, "body") else upload_response
            if result.get("statusCode", 200) != 200:
                return {"status": "Failed", "statusCode": result["statusCode"], "message": f"Error uploading file to S3: {result['message']}"}
            imagePath = result.get("assetName")

        cursor.execute("""INSERT INTO items (itemName,itemCode,itemImage,units,make,model,serialNumberFlag,barcodeFlag,createdBy,createdTime) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (item.itemName.strip(), item.itemCode.strip().upper(), imagePath, units, item.make, item.model, item.serialNumberFlag, item.barcodeFlag, item.createdBy, createdTime))
        itemId = cursor.lastrowid

        for usedForId in item.usedForIds or []:
            cursor.execute("""INSERT INTO item_used_for_mapping (itemId, usedForId, createdBy, createdTime) VALUES (%s,%s,%s,%s)""", (itemId, usedForId, item.createdBy, createdTime))

        for link in item.purchaseItemLinks or []:
            cursor.execute("""INSERT INTO item_purchase_links (itemId, purchaseLink, createdBy, createdTime) VALUES (%s,%s,%s,%s)""", (itemId, link.strip(), item.createdBy, createdTime))

        conn.commit()
        return {"status": "Success", "statusCode": 200, "data": {"itemId": itemId, "itemCode": item.itemCode}, "message": "Item added successfully"}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"Error while adding item: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_items_list(page: Optional[int], pageSize: Optional[int]):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)        
        cursor.execute("SELECT COUNT(*) AS total FROM items WHERE active = 'T'")
        total_records = cursor.fetchone()["total"]

        if page and pageSize:
            offset = (page - 1) * pageSize
            cursor.execute(""" SELECT id FROM items WHERE active = 'T' ORDER BY id DESC LIMIT %s OFFSET %s """, (pageSize, offset))
        else:
            cursor.execute(""" SELECT id FROM items WHERE active = 'T' ORDER BY id DESC """)
            page = 1
            pageSize = total_records

        item_rows = cursor.fetchall()
        item_ids = [row["id"] for row in item_rows]

        if not item_ids: return {"status": "Success","statusCode": 200, "data": [], "pagination": { "currentPage": page,"pageSize": pageSize,"totalRecords": total_records,"totalPages": 0}}

        format_strings = ",".join(["%s"] * len(item_ids))
        cursor.execute(f"""
            SELECT i.id, i.itemName, i.itemCode,  mdu.value AS units,  i.make, i.model,i.serialNumberFlag, i.barcodeFlag ,mdus.value AS usedFor
            FROM items i LEFT JOIN item_used_for_mapping ium   ON ium.itemId = i.id AND ium.active = 'T'
            LEFT JOIN ( SELECT key_id, value  FROM metadata.metadata_details WHERE metadata_types_id = (SELECT id FROM metadata.metadata_master WHERE type = 'Inv_Units')) mdu ON mdu.key_id = i.units
            LEFT JOIN ( SELECT key_id, value  FROM metadata.metadata_details  WHERE metadata_types_id = (SELECT id FROM metadata.metadata_master WHERE type = 'Inv_UsedFor')) mdus ON mdus.key_id = ium.usedForId
            WHERE i.id IN ({format_strings}) ORDER BY i.id DESC """, tuple(item_ids))
        rows = cursor.fetchall()

        items_dict = {}
        for row in rows:
            item_id = row["id"]
            if item_id not in items_dict:
                items_dict[item_id] = { "id": item_id, "itemName": row["itemName"], "itemCode": row["itemCode"], "units": row["units"], "make": row["make"], "model": row["model"],"serialNumberFlag":row["serialNumberFlag"],"barcodeFlag":row["barcodeFlag"], "usedFor": []}
            if row["usedFor"]: items_dict[item_id]["usedFor"].append(row["usedFor"])

        items = []
        for item in items_dict.values():
            item["usedFor"] = ", ".join(sorted(set(item["usedFor"])))
            items.append(item)

        return { "status": "Success", "statusCode": 200,  "data": items, "pagination": { "currentPage": page, "pageSize": pageSize, "totalRecords": total_records, "totalPages": (total_records + pageSize - 1) // pageSize if pageSize else 1 } }
    except Exception as e:
        if conn: conn.rollback()
        return { "status": "Failed", "statusCode": 500,"message": str(e) }
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_item_details(itemId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM items WHERE id = %s", (itemId,))
        if not cursor.fetchone(): return {"status": "Failed", "statusCode": 404, "message": "Item not found"}

        cursor.execute("""
            SELECT i.id,i.itemName,i.itemCode,u.value as units,i.make,i.model,i.itemImage,i.remarks,i.serialNumberFlag,i.barcodeFlag,uf.value AS usedFor,pl.purchaseLink
            FROM items i LEFT JOIN item_used_for_mapping ium ON ium.itemId = i.id
            LEFT JOIN (SELECT mdd.key_id, mdd.value FROM metadata.metadata_details mdd LEFT JOIN metadata.metadata_master mdm ON mdm.id = mdd.metadata_types_id WHERE mdm.type = "Inv_UsedFor") as uf on uf.key_id = ium.usedForId
            LEFT JOIN item_purchase_links pl ON pl.itemId = i.id
            LEFT JOIN (SELECT mdd.key_id, mdd.value FROM metadata.metadata_details mdd LEFT JOIN metadata.metadata_master mdm ON mdm.id = mdd.metadata_types_id WHERE mdm.type = "Inv_Units") as u on u.key_id = i.units
            WHERE i.active = 'T' AND ium.active = 'T' AND i.id = %s;
        """, (itemId,))
        rows = cursor.fetchall()

        itemCode = rows[0]["itemCode"].split("-")
        nature, domain, partCode, made = itemCode[0], itemCode[1], itemCode[2], itemCode[3]

        cursor.execute("SELECT purchaseLink FROM item_purchase_links WHERE active = 'T' AND itemId = %s", (itemId,))
        links = cursor.fetchall()
        image_url = f"https://usstaging.ivisecurity.com/common/downloadFile_1_0?requestName=inventory&assetName={rows[0]['itemImage']}" if rows[0]["itemImage"] else None

        item_details = {"id": rows[0]["id"], "itemName": rows[0]["itemName"], "itemCode": rows[0]["itemCode"], "make": rows[0]["make"], "model": rows[0]["model"], "units": rows[0]["units"], "nature": nature, "domain": domain, "partCode": partCode, "made": made, "usedFor": ", ".join(set(row["usedFor"] for row in rows if row["usedFor"])), "purchaseLinks": list(set(link["purchaseLink"] for link in links if link["purchaseLink"])), "itemImage": image_url, "remarks": rows[0]["remarks"]}
        return {"status": "Success", "statusCode": 200, "data": item_details}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"Error while retrieving item details: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_distinct_item():
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""SELECT DISTINCT i.itemName FROM items i WHERE i.active = 'T' ORDER BY i.itemName""")
        return {"status": "Success", "statusCode": 200,"message": "Distinct values retrieved successfully" , "data":  [row["itemName"] for row in cursor.fetchall()]}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"Error while retrieving distinct values: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

async def update_item(itemUpdateRequest: str, file):
    conn = None
    cursor = None
    try:
        req = ItemUpdateRequest(**json.loads(itemUpdateRequest))
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        modifiedTime = req.modifiedTime or datetime.now()

        cursor.execute("SELECT id,itemCode FROM items WHERE id = %s", (req.itemId,))
        item = cursor.fetchone()
        if not item: return {"status": "Failed", "statusCode": 404, "message": "Item not found"}

        if file:
            asset_name = f"{item['itemCode']}_{file.filename.split('.')[0]}"
            upload_response = await upload_file_to_s3_handler(assetFile=file, requestName="inventory", assetName=asset_name)
            result = json.loads(upload_response.body) if hasattr(upload_response, "body") else upload_response
            if result.get("statusCode", 200) != 200: return {"status": "Failed", "statusCode": result["statusCode"], "message": f"Error uploading file to S3: {result['message']}"}
            cursor.execute("UPDATE items SET itemImage=%s, modifiedBy=%s, modifiedTime=%s WHERE id=%s", (result.get("assetName"), req.modifiedBy, modifiedTime, req.itemId))

        if req.usedForIds is not None:
            cursor.execute("SELECT usedForId FROM item_used_for_mapping WHERE itemId = %s", (req.itemId,))
            existing = {row["usedForId"] for row in cursor.fetchall()}
            new_ids = set(req.usedForIds)
            to_deact = existing - new_ids
            to_add = new_ids - existing

            if to_deact: cursor.execute(f"UPDATE item_used_for_mapping SET active='F', modifiedBy=%s, modifiedTime=%s WHERE itemId=%s AND usedForId IN ({','.join(['%s']*len(to_deact))})", (req.modifiedBy, modifiedTime, req.itemId, *to_deact))
            for uid in to_add: cursor.execute("INSERT INTO item_used_for_mapping (itemId, usedForId, createdBy, createdTime) VALUES (%s,%s,%s,%s)", (req.itemId, uid, req.modifiedBy, modifiedTime))
            for uid in existing & new_ids: cursor.execute("UPDATE item_used_for_mapping SET active='T', modifiedBy=%s, modifiedTime=%s WHERE itemId=%s AND usedForId=%s", (req.modifiedBy, modifiedTime, req.itemId, uid))

        if req.purchaseItemLinks is not None:
            cursor.execute("SELECT purchaseLink FROM item_purchase_links WHERE itemId = %s", (req.itemId,))
            existing_links = {row["purchaseLink"] for row in cursor.fetchall()}
            new_links = set(link.strip() for link in req.purchaseItemLinks)
            to_deactivate_links = existing_links - new_links
            to_add_links = new_links - existing_links

            if to_deactivate_links: cursor.execute(f"UPDATE item_purchase_links SET active='F', modifiedBy=%s, modifiedTime=%s WHERE itemId=%s AND purchaseLink IN ({','.join(['%s']*len(to_deactivate_links))})", (req.modifiedBy, modifiedTime, req.itemId, *to_deactivate_links))
            for link in to_add_links: cursor.execute("INSERT INTO item_purchase_links (itemId, purchaseLink, createdBy, createdTime) VALUES (%s,%s,%s,%s)", (req.itemId, link.strip(), req.modifiedBy, modifiedTime))
            for link in existing_links & new_links: cursor.execute("UPDATE item_purchase_links SET active='T', modifiedBy=%s, modifiedTime=%s WHERE itemId=%s AND purchaseLink=%s", (req.modifiedBy, modifiedTime, req.itemId, link.strip()))

        if req.remarks is not None:
            cursor.execute("UPDATE items SET remarks=%s, modifiedBy=%s, modifiedTime=%s WHERE id=%s", (req.remarks.strip(), req.modifiedBy, modifiedTime, req.itemId))

        conn.commit()
        return {"status": "Success", "statusCode": 200, "message": "Item updated successfully"}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"Error while updating item: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_all_inventory_items(pageNo: int, pageSize: int, search: Optional[str]):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        offset = (pageNo - 1) * pageSize
        base_where = "WHERE 1=1 "
        params = []

        if search:
            base_where += """ AND ( i.itemName LIKE %s OR pi.serialNumber LIKE %s OR pi.barcode LIKE %s OR p.invoiceNumber LIKE %s) """
            params.extend([f"%{search}%"]*4)

        cursor.execute(f"SELECT COUNT(DISTINCT pi.id) as totalRecords FROM purchase_items pi JOIN items i ON i.id = pi.itemId JOIN purchase_invoices p ON p.id = pi.purchaseId {base_where}", params)
        totalRecords = cursor.fetchone()["totalRecords"]

        cursor.execute(f"""
            SELECT pi.id AS purchaseItemId, i.id AS itemId, i.itemName, i.make, i.model, CASE WHEN i.itemImage IS NULL OR i.itemImage = '' THEN NULL ELSE CONCAT('https://usstaging.ivisecurity.com/common/downloadFile_1_0?requestName=inventory&assetName=', i.itemImage) END AS itemImage, pi.serialNumber, pi.barcode, DATE_FORMAT(p.invoiceDate, '%d %b, %Y') AS purchaseDate, p.invoiceNumber, l.locationId, l.name AS locationName, l.entityType, l.country, pi.status
            FROM purchase_items pi JOIN items i ON i.id = pi.itemId JOIN purchase_invoices p ON p.id = pi.purchaseId
            LEFT JOIN (SELECT id AS locationId, entityType, name, country, status FROM locations UNION ALL SELECT siteId AS locationId, 'site', siteName, country, status FROM vip_sites_management.sites) l ON l.locationId = p.purchaseToId AND l.status IN ('T', 'Active')
            {base_where} ORDER BY pi.id DESC LIMIT %s OFFSET %s
        """, params + [pageSize, offset])
        rows = cursor.fetchall()
        result = []

        for row in rows:
            if row["status"] == "DELIVERED": status_label, color = "NEW", "#53BF8B"
            elif row["status"] == "RETURNED": status_label, color = "RETURNED", "#ED3237"
            elif row["status"] == "PREORDER": status_label, color = "Preorder", "#000000"
            else: status_label, color = row["status"], "#FFC400"

            cursor.execute("SELECT id AS purchaseLinkId, purchaseLink FROM item_purchase_links WHERE itemId = %s AND active = 'T'", (row["itemId"],))
            result.append({
                "purchaseItemId": row["purchaseItemId"], "itemId": row["itemId"], "itemName": row["itemName"], "make": row["make"], "model": row["model"], "itemImage": row["itemImage"], "serialNumber": row["serialNumber"], "barcode": row["barcode"], "qty": 1, "purchaseDate": row["purchaseDate"], "invoiceNumber": row["invoiceNumber"], "locationId": row["locationId"], "locationName": row["locationName"], "entityType": row["entityType"], "country": row["country"], "status": status_label, "statusColor": color, "purchaseLinks": cursor.fetchall()
            })

        return {"status": "Success", "statusCode": 200, "data": result, "pagination": {"pageNo": pageNo, "pageSize": pageSize, "totalRecords": totalRecords, "totalPages": (totalRecords + pageSize - 1) // pageSize}}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "Failed", "statusCode": 500, "message": f"Error while fetching inventory items: {str(e)}", "data": []}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_inventory_item_details(purchaseItemId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT pi.id AS purchaseItemId, MAX(i.id) AS itemId,MAX(i.itemName) AS itemName, MAX(i.itemCode) AS itemCode, MAX(i.make) AS make, MAX(i.model) AS model, MAX(u.value) AS unit,MAX(pi.serialNumber) AS serialNumber,MAX(pi.barcode) AS barcode, MAX(pi.unitPrice) AS unitPrice, MAX(pi.gstPercent) AS gstPercent, MAX(pi.status) AS status, MAX(p.invoiceDate) AS invoiceDate,MAX(p.invoiceNumber) AS invoiceNumber, GROUP_CONCAT(DISTINCT uf.value SEPARATOR ', ') AS usedFor FROM purchase_items pi JOIN items i ON i.id = pi.itemId JOIN purchase_invoices p ON p.id = pi.purchaseId LEFT JOIN metadata.metadata_details u ON u.key_id = i.units AND u.metadata_types_id = ( SELECT id FROM metadata.metadata_master  WHERE type='Inv_Units') LEFT JOIN item_used_for_mapping ium  ON ium.itemId = i.id AND ium.active='T' LEFT JOIN metadata.metadata_details uf ON uf.key_id = ium.usedForId AND uf.metadata_types_id = ( SELECT id FROM metadata.metadata_master  WHERE type='Inv_UsedFor') WHERE pi.id = %s GROUP BY pi.id
        """, (purchaseItemId,))
        item = cursor.fetchone()
        if not item: return {"status": "Failed","statusCode": 404, "message": "Item not found"}

        total_cost = item["unitPrice"] + (item["unitPrice"] * item["gstPercent"] / 100)
        cursor.execute("SELECT p.purchaseFromId,l.name AS purchaseFromName, l.entityType FROM purchase_items pi JOIN purchase_invoices p ON p.id = pi.purchaseId LEFT JOIN locations l ON l.id = p.purchaseFromId WHERE pi.id = %s", (purchaseItemId,))
        purchase_from = cursor.fetchone()

        cursor.execute("""
            SELECT sl.createdTime, sl.action,sl.movementType, l.locationId,l.name AS locationName, l.entityType FROM stock_ledger sl LEFT JOIN ( SELECT id AS locationId, entityType, name, status FROM locations  UNION ALL SELECT siteId AS locationId, 'site' AS entityType, siteName AS name, status FROM vip_sites_management.sites ) l ON l.locationId = sl.stockHolderId WHERE sl.actionItemId = %s ORDER BY sl.createdTime ASC
        """, (purchaseItemId,))
        movements = cursor.fetchall()
        timeline = []
        previous_location = {"locationId": purchase_from["purchaseFromId"],"locationName": purchase_from["purchaseFromName"],"entityType": purchase_from["entityType"]} if purchase_from else None
        action_label_map = {"PURCHASE": "Purchased", "ISSUE": "Issued","RETURN": "Returned", "USED": "Used","PREORDER": "Preordered","OPENING": "Opening"}

        for move in movements:
            current_location = { "locationId": move["locationId"], "locationName": move["locationName"],"entityType": move["entityType"]}
            timeline.append({"date": move["createdTime"].strftime("%d/%m/%Y"),"from": previous_location, "to": current_location,"condition": move["movementType"] or "New", "action": action_label_map.get(move["action"], move["action"])})
            previous_location = current_location

        status_color_map = { "DELIVERED": "#53BF8B", "ISSUED": "#FFC400", "RETURNED": "#ED3237","PREORDER": "#000000","USED": "#FFC400"}
        return {"status": "Success", "statusCode": 200, "data": {"header": {"title": item["itemName"], "subtitle": f'{item["make"]} - {item["model"]}'}, "itemDetails": {"purchaseItemId": item["purchaseItemId"], "itemId": item["itemId"], "itemCode": item["itemCode"], "unit": item["unit"], "make": item["make"], "model": item["model"], "serialNumber": item["serialNumber"], "barcode": item["barcode"], "usedFor": item["usedFor"], "status": item["status"], "statusColor": status_color_map.get(item["status"], "#999999"), "invoiceNumber": item["invoiceNumber"], "purchaseDate": item["invoiceDate"], "itemCostWithGST": round(total_cost, 2)}, "timeline": timeline}}
    except Exception as e:
        if conn:  conn.rollback()
        return { "status": "Failed", "statusCode": 500,"message": f"Error fetching item details: {str(e)}"}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()