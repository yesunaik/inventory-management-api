from typing import Optional
from datetime import datetime
from app.db.db import dbconn_inventory

def get_stock_summary(page: int, pageSize: int, search: str, startDate: datetime, endDate: datetime, storeId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)
        offset = (page - 1) * pageSize

        ledger_conditions = ["sl.status = 'T'"]
        ledger_params = []
        if storeId:
            ledger_conditions.append("sl.stockHolderId = %s")
            ledger_params.append(storeId)

        ledger_where = "WHERE " + " AND ".join(ledger_conditions) if ledger_conditions else ""
        search_filter, search_params = "", []

        if search:
            search_filter = """WHERE i.itemName LIKE %s OR i.itemCode LIKE %s OR i.make LIKE %s OR i.model LIKE %s"""
            search_params = [f"%{search}%"] * 4

        preorder_filter, preorder_params = "", []
        if storeId:
            preorder_filter = "AND p.purchaseToId = %s"
            preorder_params.append(storeId)

        query = f"""
        SELECT i.id AS itemId, MAX(i.itemName) AS itemName, MAX(i.itemCode) AS itemCode, MAX(i.make) AS make, MAX(i.model) AS model, MAX(units.value) AS unitsName, GROUP_CONCAT(DISTINCT usedFor.value SEPARATOR ', ') AS usedFor, COALESCE(sl.opening,0) AS opening, COALESCE(sl.purchase,0) AS purchase, COALESCE(sl.used,0) AS used, COALESCE(sl.issued,0) AS issued, COALESCE(sl.returned,0) AS returned, ( COALESCE(sl.opening,0) + COALESCE(sl.purchase,0) + COALESCE(sl.returned,0) - COALESCE(sl.issued,0) - COALESCE(sl.used,0) ) AS closing, COALESCE(po.preorder,0) AS preorder
        FROM items i
        LEFT JOIN (
            SELECT sl.itemId,
            SUM( CASE WHEN sl.createdTime < %s THEN CASE WHEN sl.action='OPENING' THEN sl.qtyIn - sl.qtyOut WHEN sl.action='PURCHASE' THEN sl.qtyIn WHEN sl.action='RETURN' THEN sl.qtyIn WHEN sl.action IN ('ISSUE','USED') THEN -sl.qtyOut ELSE 0 END ELSE 0 END ) AS opening,
            SUM( CASE WHEN sl.action='PURCHASE' AND sl.createdTime BETWEEN %s AND %s THEN sl.qtyIn ELSE 0 END ) AS purchase,
            SUM( CASE WHEN sl.action='USED' AND sl.createdTime BETWEEN %s AND %s THEN sl.qtyOut ELSE 0 END ) AS used,
            SUM( CASE WHEN sl.action='ISSUE' AND sl.createdTime BETWEEN %s AND %s THEN sl.qtyOut ELSE 0 END ) AS issued,
            SUM( CASE WHEN sl.action='RETURN' AND sl.createdTime BETWEEN %s AND %s THEN sl.qtyIn ELSE 0 END ) AS returned
            FROM stock_ledger sl {ledger_where} GROUP BY sl.itemId
        ) sl ON sl.itemId = i.id
        LEFT JOIN (
            SELECT pi.itemId, COUNT(*) preorder FROM purchase_items pi JOIN purchase_invoices p ON p.id = pi.purchaseId WHERE pi.status = 'PREORDER' {preorder_filter} GROUP BY pi.itemId
        ) po ON po.itemId = i.id
        LEFT JOIN item_used_for_mapping ium ON ium.itemId = i.id AND ium.active = 'T'
        LEFT JOIN metadata.metadata_master mu ON mu.type = 'Inv_UsedFor'
        LEFT JOIN metadata.metadata_details usedFor ON usedFor.key_id = ium.usedForId AND usedFor.metadata_types_id = mu.id
        LEFT JOIN metadata.metadata_master mu2 ON mu2.type = 'Inv_Units'
        LEFT JOIN metadata.metadata_details units ON units.key_id = i.units AND units.metadata_types_id = mu2.id
        {search_filter} GROUP BY i.id ORDER BY i.id DESC LIMIT %s OFFSET %s
        """
        params = [startDate, startDate, endDate, startDate, endDate, startDate, endDate, startDate, endDate] + ledger_params + preorder_params + search_params + [pageSize, offset]
        cursor.execute(query, params)
        rows = cursor.fetchall()

        count_query = "SELECT COUNT(*) total FROM items i"
        count_params = []
        if search:
            count_query += """ WHERE i.itemName LIKE %s OR i.itemCode LIKE %s OR i.make LIKE %s OR i.model LIKE %s"""
            count_params = [f"%{search}%"] * 4
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()["total"]

        return {"status": "Success", "statusCode": 200, "message": "Stock summary retrieved successfully", "data": rows, "pagination": { "page": page, "pageSize": pageSize, "totalRecords": total, "totalPages": (total + pageSize - 1) // pageSize}}
    except Exception as e:
        return {"status": "Failed", "statusCode": 500, "message": f"Error retrieving stock summary: {str(e)}", "data": None}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_closing_statement(itemId: int, startDate: datetime, endDate: datetime, storeId: int):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)

        header_query = """SELECT i.itemName, COALESCE(SUM(sl.qtyIn),0) - COALESCE(SUM(sl.qtyOut),0) AS availableCount FROM items i LEFT JOIN stock_ledger sl ON sl.itemId = i.id AND sl.status = 'T'"""
        params = []
        if storeId:
            header_query += " AND sl.stockHolderId = %s"
            params.append(storeId)
        header_query += " WHERE i.id = %s GROUP BY i.id"
        params.append(itemId)

        cursor.execute(header_query, params)
        header = cursor.fetchone()

        movement_query = """SELECT DATE(sl.createdTime) AS date, sl.action, SUM( CASE WHEN sl.qtyIn > 0 THEN sl.qtyIn ELSE sl.qtyOut END ) AS qty, l.name AS locationName FROM stock_ledger sl LEFT JOIN locations l ON l.id = sl.stockHolderId WHERE sl.itemId = %s AND sl.status = 'T' AND sl.createdTime BETWEEN %s AND %s"""
        params = [itemId, startDate, endDate]
        if storeId:
            movement_query += " AND sl.stockHolderId = %s"
            params.append(storeId)
        movement_query += " GROUP BY DATE(sl.createdTime), sl.action, l.name ORDER BY DATE(sl.createdTime)"
        
        cursor.execute(movement_query, params)
        rows = cursor.fetchall()

        running_stock = 0
        timeline = []
        for r in rows:
            qty = r["qty"]
            action = r["action"]
            location = r["locationName"]

            if action in ("PURCHASE", "RETURN"): running_stock += qty
            else: running_stock -= qty

            if action == "PURCHASE": from_loc, to_loc, status = "Vendor", location, "New"
            elif action == "ISSUE": from_loc, to_loc, status = location, "Site", "New"
            elif action == "USED": from_loc, to_loc, status = location, "Used", "Used"
            elif action == "RETURN": from_loc, to_loc, status = "Site", location, "Used"
            else: from_loc, to_loc, status = location, location, action

            timeline.append({"date": r["date"], "from": from_loc, "to": to_loc, "status": status, "count": qty, "action": action, "availableCount": running_stock})

        timeline.reverse()
        return {"status": "Success", "statusCode": 200, "data": {"header": {"itemName": header["itemName"], "availableCount": header["availableCount"], "startDate": startDate, "endDate": endDate}, "details": timeline}}
    except Exception as e:
        return {"status": "Failed", "statusCode": 500, "message": str(e)}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()