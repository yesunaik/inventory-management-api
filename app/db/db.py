import mysql.connector

def dbconn_inventory():
    try:
        conn = mysql.connector.connect(
            host= "localhost",
            user= "root",
            password= "root",
            database= "inventory")
        return conn
    except mysql.connector.Error as err:
        output = {"status": "Failed", "statusCode": 501,"message": f"Error while connecting database: {str(err)}"}
        return output


def dbconn_common():
    try:
        conn = mysql.connector.connect(
            host= "localhost",
            user= "root",
            password= "root",
            database='common')
        return conn
    except mysql.connector.Error as err:
        output = {"status": "Failed", "statusCode": 501,"message": f"Error while connecting database: {str(err)}"}
        return output

