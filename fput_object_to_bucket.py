from minio import Minio
from minio.error import S3Error

if __name__ == '__main__':
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )

    bucket_name = "uploads"

    file_path = "files/test.txt"
    object_name = "tests/test.txt"

    try:
        # TODO: загрузить файл в MinIO
        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
        )
        print(f"Файл {file_path} загружен в MinIO, объект: {object_name}")
    except S3Error as e:
        print("Ошибка работы с MinIO:", e)
