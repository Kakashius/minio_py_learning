from fastapi import FastAPI, UploadFile, File
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse

import asyncio
from minio import Minio
from minio.error import S3Error
import uuid
import urllib.parse

app = FastAPI()

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)

BUCKET_NAME = "uploads"

ALLOWED_MIME_TYPES = {
    "image/png",
    "image/jpeg",
    "application/pdf",
}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    if file.content_type not in ALLOWED_MIME_TYPES:
        raise HTTPException(
            status_code=400,
            detail="Недопустимый тип файла",
        )

    unique_name = str(uuid.uuid4()) + "." + file.filename.split(".")[-1]

    object_name = f"files/{unique_name}"

    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=file.file,
        length=-1,
        part_size=10 * 1024 * 1024,
        content_type=file.content_type,
    )

    return {"object_name": object_name}

@app.get("/download/{object_name:path}")    # :path - чтобы в object_name могли быть "/"
def download_file(object_name: str):
    try:
        obj = client.get_object(
            BUCKET_NAME,
            object_name,
        )

        filename = object_name.split("/")[-1]
        encoded_filename = urllib.parse.quote(filename)

        return StreamingResponse(
            obj,
            media_type="application/octet-stream",  # ← любые бинарные данные, универсально
            headers={
                "Content-Disposition": f'attachment; filename="{encoded_filename}"'
            },
        )

    except Exception:
        raise HTTPException(status_code=404, detail="Файл не найден")


@app.delete("/delete/{object_name:path}")
def delete_file(object_name: str):
    try:
        client.remove_object(BUCKET_NAME, object_name)
        return {"detail": "Файл успешно удалён"}
    except S3Error as e:
        # e.code может быть "NoSuchKey", "AccessDenied" и т.д.
        raise HTTPException(status_code=404, detail=f"Файл не найден или ошибка при удалении, {str(e)}")

PART_SIZE = 10 * 1024 * 1024  # 10 MB


async def async_upload_part(bucket, object_name, upload_id, part_number, data):
    loop = asyncio.get_running_loop()
    # Запускаем синхронный upload_part в отдельном потоке
    return await loop.run_in_executor(
        None,
        *[lambda: client._upload_part(bucket, object_name, upload_id, part_number, data)]
    )

# Multipart uploading для загрузки в MinIO больших файлов
@app.post("/upload/multipart")
async def multipart_upload(file: UploadFile = File(...)):
    object_name = f"files/{file.filename}"

    try:
        # 1. Создаем multipart upload
        upload_id = client._create_multipart_upload(BUCKET_NAME, object_name)
        parts = []
        part_number = 1

        while True:
            data = await file.read(PART_SIZE)
            if not data:
                break

            # 2. Загружаем часть асинхронно
            retries = 3
            while retries > 0:
                try:
                    etag = await async_upload_part(BUCKET_NAME, object_name, upload_id, part_number, data)
                    parts.append({"PartNumber": part_number, "ETag": etag})
                    break  # если успешно, выходим из цикла повторов
                except S3Error:
                    retries -= 1
                    if retries == 0:
                        raise HTTPException(status_code=500, detail=f"Не удалось загрузить часть {part_number}")

            part_number += 1

        # 3. Завершаем multipart upload
        client._complete_multipart_upload(BUCKET_NAME, object_name, upload_id, parts)
        return {"detail": "Файл успешно загружен", "object_name": object_name}

    except S3Error as err:
        raise HTTPException(status_code=500, detail=f"Ошибка MinIO: {err}")

# -------------------------------
# Список всех версий объекта
# -------------------------------
@app.get("/versions/{object_name}")
def list_versions(object_name: str):
    try:
        versions = [
            {"version_id": obj.version_id, "is_latest": obj.is_latest}
            for obj in client.list_objects(BUCKET_NAME, prefix=object_name, versions=True)
        ]
        return {"versions": versions}
    except S3Error:
        raise HTTPException(status_code=404, detail="Файл не найден")

# -------------------------------
# Скачивание конкретной версии
# -------------------------------
@app.get("/download/{object_name}/version/{version_id}")
def download_version(object_name: str, version_id: str):
    try:
        obj = client.get_object(BUCKET_NAME, object_name, version_id=version_id)
        return obj.read()
    except S3Error:
        raise HTTPException(status_code=404, detail="Версия не найдена")

# -------------------------------
# Удаление конкретной версии
# -------------------------------
@app.delete("/delete/{object_name}/version/{version_id}")
def delete_version(object_name: str, version_id: str):
    try:
        client.remove_object(BUCKET_NAME, object_name, version_id=version_id)
        return {"detail": f"Версия {version_id} удалена"}
    except S3Error:
        raise HTTPException(status_code=404, detail="Версия не найдена")