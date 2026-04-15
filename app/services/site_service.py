from app.db.db import dbconn_inventory

def get_site_inventory(siteId: int, viewType: str):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)

        if viewType.upper() == "NORMAL":
            query = """
            SELECT pd.id, p.productName AS name, 'Product' AS type, p.make, p.model, SUM( CASE WHEN iss.status IN ('ISSUED','DELIVERED') THEN 1 ELSE 0 END ) AS delivered, ( SELECT COUNT(*) FROM return_products rp JOIN returns r ON r.id = rp.returnId JOIN issue_products ip ON ip.id = rp.issueProductId WHERE ip.productDetailsId = pd.id AND r.returnFromId = %s AND r.status='RETURNED' ) AS returned FROM product_details pd JOIN products p ON p.id = pd.productId JOIN issue_products ip ON ip.productDetailsId = pd.id JOIN issued iss ON iss.id = ip.issueId WHERE iss.issuedToId = %s GROUP BY pd.id,p.productName,p.make,p.model
            UNION ALL
            SELECT ii.itemId AS id, i.itemName AS name, 'Item' AS type, i.make, i.model, COUNT(*) AS delivered, ( SELECT COUNT(*) FROM return_items ri JOIN returns r ON r.id = ri.returnId JOIN issue_items ii2 ON ii2.id = ri.issueItemId WHERE ii2.itemId = ii.itemId AND r.returnFromId = %s AND r.status='RETURNED' ) AS returned FROM issue_items ii JOIN items i ON i.id = ii.itemId JOIN issued iss ON iss.id = ii.issueId WHERE iss.issuedToId = %s GROUP BY ii.itemId,i.itemName,i.make,i.model
            """
            cursor.execute(query,(siteId,siteId,siteId,siteId))
            return {"status":"Success", "statusCode":200, "viewType":"NORMAL", "data":cursor.fetchall()}
        else:
            query = """
            SELECT DATE(iss.issueDate) AS activityDate, i.itemName AS name, 'Item' AS type, i.make, i.model, ii.serialNumber, ii.barcode, 'ISSUED' AS action, 1 AS storeQty, 0 AS onlineQty, 1 AS totalQty, md.value AS units FROM issue_items ii JOIN issued iss ON iss.id = ii.issueId JOIN items i ON i.id = ii.itemId LEFT JOIN metadata.metadata_master mm ON mm.type='Inv_Units' LEFT JOIN metadata.metadata_details md ON md.metadata_types_id = mm.id AND md.key_id = i.units WHERE iss.issuedToId = %s
            UNION ALL
            SELECT DATE(iss.issueDate) AS activityDate, p.productName AS name, 'Product' AS type, p.make, p.model, pd.serialNumber, pd.barCode, 'ISSUED' AS action, 1 AS storeQty, 0 AS onlineQty, 1 AS totalQty, md.value AS units FROM issue_products ip JOIN issued iss ON iss.id = ip.issueId JOIN product_details pd ON pd.id = ip.productDetailsId JOIN products p ON p.id = pd.productId LEFT JOIN metadata.metadata_master mm ON mm.type='Inv_Units' LEFT JOIN metadata.metadata_details md ON md.metadata_types_id = mm.id AND md.key_id = p.units WHERE iss.issuedToId = %s
            UNION ALL
            SELECT DATE(r.returnDate) AS activityDate, i.itemName AS name, 'Item' AS type, i.make, i.model, ii.serialNumber, ii.barcode, 'RETURNED' AS action, 0 AS storeQty, 1 AS onlineQty, 1 AS totalQty, md.value AS units FROM return_items ri JOIN returns r ON r.id = ri.returnId JOIN issue_items ii ON ii.id = ri.issueItemId JOIN items i ON i.id = ii.itemId LEFT JOIN metadata.metadata_master mm ON mm.type='Inv_Units' LEFT JOIN metadata.metadata_details md ON md.metadata_types_id = mm.id AND md.key_id = i.units WHERE r.returnFromId = %s
            UNION ALL
            SELECT DATE(r.returnDate) AS activityDate, p.productName AS name, 'Product' AS type, p.make, p.model, pd.serialNumber, pd.barCode, 'RETURNED' AS action, 0 AS storeQty, 1 AS onlineQty, 1 AS totalQty, md.value AS units FROM return_products rp JOIN returns r ON r.id = rp.returnId JOIN issue_products ip ON ip.id = rp.issueProductId JOIN product_details pd ON pd.id = ip.productDetailsId JOIN products p ON p.id = pd.productId LEFT JOIN metadata.metadata_master mm ON mm.type='Inv_Units' LEFT JOIN metadata.metadata_details md ON md.metadata_types_id = mm.id AND md.key_id = p.units WHERE r.returnFromId = %s ORDER BY activityDate DESC
            """
            cursor.execute(query,(siteId,siteId,siteId,siteId))
            return {"status":"Success", "statusCode":200, "viewType":"DETAILED", "data":cursor.fetchall()}
    except Exception as e:
        return {"status":"Failed", "statusCode":500, "message":str(e)}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_site_inventory_item_details(siteId: int, itemId: int, type: str):
    conn = None
    cursor = None
    try:
        conn = dbconn_inventory()
        cursor = conn.cursor(dictionary=True)

        if type.upper() == "ITEM":
            headerQuery = """SELECT s.siteName, i.itemName, i.make, i.model FROM items i JOIN issue_items ii ON ii.itemId = i.id JOIN issued iss ON iss.id = ii.issueId JOIN vip_sites_management.sites s ON s.siteId = iss.issuedToId WHERE i.id = %s AND iss.issuedToId = %s LIMIT 1"""
            cursor.execute(headerQuery,(itemId,siteId))
            header = cursor.fetchone()

            query = """
            SELECT DATE(iss.issueDate) AS date, SUM(ii.quantity) AS deliveredFromStore, 0 AS deliveredFromOnline, 0 AS returnedToStore, 0 AS returnedToOnline FROM issue_items ii JOIN issued iss ON iss.id = ii.issueId WHERE iss.issuedToId = %s AND ii.itemId = %s GROUP BY DATE(iss.issueDate)
            UNION ALL
            SELECT DATE(r.returnDate) AS date, 0, 0, COUNT(*) AS returnedToStore, 0 FROM return_items ri JOIN returns r ON r.id = ri.returnId JOIN issue_items ii ON ii.id = ri.issueItemId WHERE r.returnFromId = %s AND ii.itemId = %s GROUP BY DATE(r.returnDate) ORDER BY date DESC
            """
            cursor.execute(query,(siteId,itemId,siteId,itemId))

        else:
            headerQuery = """SELECT s.siteName, p.productName AS itemName, p.make, p.model FROM products p JOIN product_details pd ON pd.productId = p.id JOIN issue_products ip ON ip.productDetailsId = pd.id JOIN issued iss ON iss.id = ip.issueId JOIN vip_sites_management.sites s ON s.siteId = iss.issuedToId WHERE pd.id = %s AND iss.issuedToId = %s LIMIT 1"""
            cursor.execute(headerQuery,(itemId,siteId))
            header = cursor.fetchone()

            query = """
            SELECT DATE(iss.issueDate) AS date, COUNT(*) AS deliveredFromStore, 0 AS deliveredFromOnline, 0 AS returnedToStore, 0 AS returnedToOnline FROM issue_products ip JOIN issued iss ON iss.id = ip.issueId WHERE iss.issuedToId = %s AND ip.productDetailsId = %s GROUP BY DATE(iss.issueDate)
            UNION ALL
            SELECT DATE(r.returnDate) AS date, 0, 0, COUNT(*) AS returnedToStore, 0 FROM return_products rp JOIN returns r ON r.id = rp.returnId JOIN issue_products ip ON ip.id = rp.issueProductId WHERE r.returnFromId = %s AND ip.productDetailsId = %s GROUP BY DATE(r.returnDate) ORDER BY date DESC
            """
            cursor.execute(query,(siteId,itemId,siteId,itemId))

        details = cursor.fetchall()
        return {"status":"Success", "statusCode":200, "data":{"header":header, "details":details}}
    except Exception as e:
        return {"status":"Failed", "statusCode":500, "message":str(e)}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()