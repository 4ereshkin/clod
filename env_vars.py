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

settings = Settings()
