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


class Config(BaseModel):
    rabbitmq: RabbitMQConfig = Field(default_factory=lambda: RabbitMQConfig(**env))