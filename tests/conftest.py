import pytest
import pytest_asyncio

from application.common.config import AppSettings


@pytest.fixture(scope="session")
def app_settings():
    return AppSettings()


@pytest_asyncio.fixture(scope="function")
async def rabbit_connection(app_settings):
    import aio_pika
    conn = await aio_pika.connect_robust(app_settings.rabbitmq.dsn)
    yield conn
    await conn.close()
