from dataclasses import dataclass
from dotenv import load_dotenv
import os


@dataclass(frozen=True)
class Settings:

    # SoT PostgreSQL параметры
    load_dotenv(dotenv_path=r'D:\1_prod\.env')

    _pgapp_db_name: str = os.getenv("PGAPP_DB_NAME")
    _pgapp_db_user: str = os.getenv("PGAPP_DB_USER")
    _pgapp_db_password: str = os.getenv("PGAPP_DB_PASSWORD")
    _pgapp_db_port: str = os.getenv("PGAPP_DB_PORT")
    pgapp_host = '127.0.0.1'

    pg_dsn: str = f'postgresql+psycopg2://{_pgapp_db_user}:{_pgapp_db_password}@{pgapp_host}:{_pgapp_db_port}/{_pgapp_db_name}'

    # MinIO S3 хранилище для объектов пайплайнов

    _minio_port: str = os.getenv("MINIO_PORT")
    _minio_host: str = f'http://127.0.0.1:{_minio_port}'

    s3_endpoint: str = os.getenv("S3_ENDPOINT", _minio_host)
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", os.getenv('MINIO_ROOT_USER'))
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", os.getenv('MINIO_ROOT_PASSWORD'))
    s3_bucket: str = os.getenv("S3_BUCKET", "lidar-data")
    s3_region: str = os.getenv("S3_REGION", "us-east-1")


settings = Settings()
