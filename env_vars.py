from dataclasses import dataclass
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

load_dotenv()

@dataclass(frozen=True)
class Settings:
    keydb_dsn: str = os.getenv("KEYDB_DSN", "redis://localhost:6379")
    rabbit_dsn: str = os.getenv("RABBIT_DSN", "amqp://guest:guest@localhost:5672/")
    temporal_dsn: str = os.getenv("TEMPORAL_DSN", "localhost:7233")
    pg_dsn: str = os.getenv("PG_DSN", "postgresql://user:password@localhost:5432/db")

    # S3 / MinIO
    s3_endpoint: str = os.getenv("S3_ENDPOINT", f"http://127.0.0.1:{os.getenv('MINIO_PORT', '9000')}")
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
    s3_bucket: str = os.getenv("S3_BUCKET", "lidar-data")
    s3_region: str = os.getenv("S3_REGION", "us-east-1")

settings = Settings()
