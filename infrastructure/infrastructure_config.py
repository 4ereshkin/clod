from pydantic import Field, BaseModel
from os import environ as env


class RabbitMQConfig(BaseModel):
    host: str = Field(alias="RABBITMQ_HOST")
    port: int = Field(alias="RABBITMQ_PORT")
    login: str = Field(alias="RABBITMQ_USER")
    password: str = Field(alias="RABBITMQ_PASSWORD")


class S3Config(BaseModel):
    access_key: str = Field(alias="S3_ACCESS_KEY")
    secret_key: str = Field(alias="S3_SECRET_KEY")
    bucket_name: str = Field(alias="S3_BUCKET")
    endpoint_url: str = Field(alias="S3_ENDPOINT_URL")
    region_name: str = Field(alias="S3_REGION_NAME", default='us-east-1')


class KeyDBConfig(BaseModel):
    host: str = Field(alias="KEYDB_HOST")
    port: int = Field(alias="KEYDB_PORT")
    password: str = Field(alias="KEYDB_PASSWORD")
    username: str = Field(alias="KEYDB_USERNAME", default='default')


class Config(BaseModel):
    rabbitmq: RabbitMQConfig = Field(default_factory=lambda: RabbitMQConfig(**env))
    s3: S3Config = Field(default_factory=lambda: S3Config(**env))
    keydb: KeyDBConfig = Field(default_factory=lambda: KeyDBConfig(**env))
