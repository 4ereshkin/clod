from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    pg_dsn: str = os.getenv("PG_DSN",
                            'postgresql+psycopg2://postgres:postgres@localhost:5433/app_db')

    s3_endpoint: str = os.getenv("S3_ENDPOINT", 'http://127.0.0.1:9000')
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", "minioadmin123")
    s3_bucket: str = os.getenv("S3_BUCKET", "lidar-data")
    s3_region: str = os.getenv("S3_REGION", "us-east-1")


settings = Settings()