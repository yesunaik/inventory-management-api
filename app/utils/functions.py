import boto3 , re ,mimetypes 
from typing import Optional
from app.db.db import dbconn_common
from fastapi import FastAPI, UploadFile
from fastapi.responses import JSONResponse

async def upload_file_to_s3_handler(
        assetFile: UploadFile,
        requestName: str,
        assetName: str,
        levels: Optional[str] = None
):
    try:
        # Step 1: Read file content and validate size
        file_content = await assetFile.read()
        file_size = len(file_content)
        max_file_size = 100 * 1024 * 1024
        if file_size > max_file_size:
            return {"statusCode": 400, "message": "File size exceeds 100MB limit."}

        # Step 2: Fetch S3 details from DB
        connection = dbconn_common()
        cursor = connection.cursor()
        cursor.callproc('s3_details', (requestName,))
        results = []

        for result in cursor.stored_results():
            rows = result.fetchall()
            column_names = [desc[0] for desc in result.description]
            for row in rows:
                results.append(dict(zip(column_names, row)))

        if not results:
            return {"statusCode": 404, "message": "No Default Path Found"}

        s3_details = results[0]

        # Step 3: Build S3 key
        key_parts = [s3_details['path'].strip('/')] if s3_details.get('path') else []
        if levels:
            key_parts.append(re.sub(r"\s+", "", levels).replace(",", "/"))

        file_extension = assetFile.filename.split('.')[-1].lower()
        asset_name_clean = re.sub(r"\s+", "", assetName)
        asset_key = f"{'/'.join(key_parts)}/{asset_name_clean}.{file_extension}"

        # Step 4: Upload to S3
        s3_client = boto3.client(
            "s3",
            region_name=s3_details["region"],
            aws_access_key_id=s3_details["access_key"],
            aws_secret_access_key=s3_details["secret_key"]
        )

        s3_client.put_object(
            Bucket=s3_details["bucket_name"],
            Key=asset_key,
            Body=file_content,
            ContentType=mimetypes.guess_type(asset_name_clean)[0] or "application/octet-stream",  # ⬅ fixed
        )

        return JSONResponse({
            "assetName": f"{asset_name_clean}.{file_extension}",
            "requestName": requestName,
            "levels": levels if levels else ""
        })

    except Exception as e:
        return {"statusCode": 500, "message": f"Error uploading to S3: {e}"}  # ⬅ aligned wording

    finally:
        cursor.close()
        connection.close()


