import os
from minio import Minio
from minio.error import S3Error

# Пример загрузки большого файла
if __name__ == '__main__':
    BUCKET_NAME = "uploads"
    OBJECT_NAME = "bigfile.zip"
    FILE_PATH = "/path/to/very_large_file.zip"
    PART_SIZE = 10 * 1024 * 1024  # PART_SIZE — размер каждой части (10 МБ — стандартно, можно больше)

    # Подключение к MinIO
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    try:
        # 1. Создать multipart upload
        upload_id = client._create_multipart_upload(    # уникальный идентификатор multipart upload
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            headers={
                "Content-Type": "application/octet-stream"
            })
        parts = []

        # 2. Загружать файл частями
        file_size = os.path.getsize(FILE_PATH)
        part_number = 1
        with open(FILE_PATH, "rb") as f:
            while True:
                data = f.read(PART_SIZE)
                if not data:
                    break

                etag = client._upload_part(     # etag → нужен для завершения сборки файла
                    bucket_name=BUCKET_NAME,
                    object_name=OBJECT_NAME,
                    upload_id=upload_id,
                    part_number=part_number,
                    data=data
                )
                parts.append({"PartNumber": part_number, "ETag": etag})
                print(f"Загружена часть {part_number}, размер {len(data)}")
                part_number += 1

        # 3. Завершить загрузку
        client._complete_multipart_upload(BUCKET_NAME, OBJECT_NAME, upload_id, parts)
        print("Файл успешно загружен!")

    except S3Error as err:
        print(f"Ошибка при загрузке: {err}")
