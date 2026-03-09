from typing import Literal, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, computed_field, ConfigDict
from urllib.parse import quote_plus

from pydantic import BaseModel

class PostgresSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    user: str = Field(alias="PGAPP_DB_USER", default="user")
    password: str = Field(alias="PGAPP_DB_PASSWORD", default="password")
    host: str = Field(alias="PGAPP_DB_HOST", default="127.0.0.1")
    port: int = Field(alias="PGAPP_DB_PORT", default=5432)
    db_name: str = Field(alias="PGAPP_DB_NAME", default="lidar_db")

    @computed_field
    def dsn(self) -> str:
        # psycopg2 requires string dsn with urlencoded credentials
        return f"postgresql+psycopg2://{quote_plus(self.user)}:{quote_plus(self.password)}@{self.host}:{self.port}/{quote_plus(self.db_name)}"


class S3Settings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    endpoint: str = Field(alias="S3_ENDPOINT", default="http://127.0.0.1:9000")
    access_key: str = Field(alias="S3_ACCESS_KEY", default="minioadmin")
    secret_key: str = Field(alias="S3_SECRET_KEY", default="minioadmin")
    bucket: str = Field(alias="S3_BUCKET", default="lidar-data")
    region: str = Field(alias="S3_REGION", default="us-east-1")


class RabbitMQSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    dsn: str = Field(alias="RABBIT_DSN", default="amqp://guest:guest@localhost:5672/")


class KeyDBSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    dsn: str = Field(alias="KEYDB_DSN", default="redis://localhost:6379")


class TemporalSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    dsn: str = Field(alias="TEMPORAL_DSN", default="localhost:7233")


from functools import lru_cache

class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    event_transport: Literal['rabbit', 'signalr'] = Field(alias="EVENT_TRANSPORT", default='rabbit')
    signalr_hub_url: str = Field(alias="SIGNALR_HUB_URL", default='http://localhost:5000/ingestHub')

    pg_user: str = Field(alias="PGAPP_DB_USER", default="user")
    pg_password: str = Field(alias="PGAPP_DB_PASSWORD", default="password")
    pg_host: str = Field(alias="PGAPP_DB_HOST", default="127.0.0.1")
    pg_port: int = Field(alias="PGAPP_DB_PORT", default=5432)
    pg_db_name: str = Field(alias="PGAPP_DB_NAME", default="lidar_db")

    s3_endpoint: str = Field(alias="S3_ENDPOINT", default="http://127.0.0.1:9000")
    s3_access_key: str = Field(alias="S3_ACCESS_KEY", default="minioadmin")
    s3_secret_key: str = Field(alias="S3_SECRET_KEY", default="minioadmin")
    s3_bucket: str = Field(alias="S3_BUCKET", default="lidar-data")
    s3_region: str = Field(alias="S3_REGION", default="us-east-1")

    rabbit_dsn: str = Field(alias="RABBIT_DSN", default="amqp://guest:guest@localhost:5672/")
    keydb_dsn: str = Field(alias="KEYDB_DSN", default="redis://localhost:6379")
    temporal_dsn: str = Field(alias="TEMPORAL_DSN", default="localhost:7233")

    @computed_field
    def postgres(self) -> PostgresSettings:
        return PostgresSettings(
            user=self.pg_user,
            password=self.pg_password,
            host=self.pg_host,
            port=self.pg_port,
            db_name=self.pg_db_name
        )

    @computed_field
    def s3(self) -> S3Settings:
        return S3Settings(
            endpoint=self.s3_endpoint,
            access_key=self.s3_access_key,
            secret_key=self.s3_secret_key,
            bucket=self.s3_bucket,
            region=self.s3_region
        )

    @computed_field
    def rabbitmq(self) -> RabbitMQSettings:
        return RabbitMQSettings(dsn=self.rabbit_dsn)

    @computed_field
    def keydb(self) -> KeyDBSettings:
        return KeyDBSettings(dsn=self.keydb_dsn)

    @computed_field
    def temporal(self) -> TemporalSettings:
        return TemporalSettings(dsn=self.temporal_dsn)

@lru_cache()
def get_settings() -> AppSettings:
    """Returns a cached instance of AppSettings to prevent redundant disk I/O when reading .env."""
    return AppSettings()
