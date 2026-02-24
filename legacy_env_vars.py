from dataclasses import dataclass
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus


@dataclass(frozen=True)
class Settings:

    # SoT PostgreSQL параметры
    load_dotenv(dotenv_path=r'/.env')

    _pgapp_db_name: str = os.getenv("PGAPP_DB_NAME") or "lidar_db"
    _pgapp_db_user: str = os.getenv("PGAPP_DB_USER") or "user"
    _pgapp_db_password: str = os.getenv("PGAPP_DB_PASSWORD") or "password"
    _pgapp_db_port: str = os.getenv("PGAPP_DB_PORT") or "5432"
    pgapp_host = '127.0.0.1'

    pg_dsn: str = f'postgresql+psycopg2://{quote_plus(str(_pgapp_db_user))}:{quote_plus(str(_pgapp_db_password))}@{pgapp_host}:{_pgapp_db_port}/{quote_plus(str(_pgapp_db_name))}'

    # MinIO S3 хранилище для объектов пайплайнов

    _minio_port: str = os.getenv("MINIO_PORT") or "9000"
    _minio_host: str = f'http://127.0.0.1:{_minio_port}'

    s3_endpoint: str = os.getenv("S3_ENDPOINT", _minio_host)
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", os.getenv('MINIO_ROOT_USER') or "minioadmin")
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", os.getenv('MINIO_ROOT_PASSWORD') or "minioadmin")
    s3_bucket: str = os.getenv("S3_BUCKET", "lidar-data")
    s3_region: str = os.getenv("S3_REGION", "us-east-1")


settings = Settings()
