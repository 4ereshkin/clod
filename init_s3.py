import asyncio
from aiobotocore.session import get_session


async def setup_s3():
    session = get_session()
    async with session.create_client(
            's3',
            endpoint_url='http://127.0.0.1:8333',
            aws_access_key_id='admin',
            aws_secret_access_key='admin',
            region_name='us-east-1'
    ) as client:
        # 1. Создаем бакет
        print("Creating bucket 'lidar-data'...")
        try:
            await client.create_bucket(Bucket='lidar-data')
            print("Bucket created.")
        except Exception as e:
            print(f"Bucket might already exist: {e}")

        # 2. Создаем фейковый .las файл и заливаем
        content = b"fake lidar binary data"
        key = "raw/scan_001.las"
        print(f"Uploading file to {key}...")

        await client.put_object(
            Bucket='lidar-data',
            Key=key,
            Body=content
        )
        print("Done! S3 is ready for ingestion.")


if __name__ == "__main__":
    asyncio.run(setup_s3())