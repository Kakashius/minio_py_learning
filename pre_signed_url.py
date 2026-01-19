from datetime import timedelta
from minio import Minio

if __name__ == '__main__':

    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )

    # Предположим, что объект уже есть
    object_name = "files/550e8400-e29b-41d4-a716-446655440000.png"

    BUCKET_NAME = "uploads"
    """
    1) presigned_get_object → для скачивания

    2) presigned_put_object → для загрузки (фронт может загружать напрямую в MinIO)
    
    3) expires → время жизни URL
    """
    # Генерация URL на скачивание
    url = client.presigned_get_object(
        BUCKET_NAME,
        object_name,
        expires=timedelta(minutes=5)  # действителен 5 минут
    )

    print(url)  # Созает прямую ссылку на скачивание файла из MinIO
