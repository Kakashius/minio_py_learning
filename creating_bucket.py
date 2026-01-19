from minio import Minio
from minio.error import S3Error

if __name__ == '__main__':
    client = Minio(
        "localhost:9000",   # API MinIO
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,               # потому что HTTP, не HTTPS
    )

    bucket_name = "uploads"

    try:
        # TODO 1: проверить, существует ли bucket
        exists = client.bucket_exists(bucket_name)

        if not exists:
            # TODO 2: создать bucket
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' создан")
        else:
            print(f"Bucket '{bucket_name}' уже существует")

    except S3Error as e:
        print("Ошибка работы с MinIO:", e)
