import base64
import hashlib
import os
from contextlib import asynccontextmanager
from aiobotocore.session import get_session

from config import S3Config
from typing import Optional

from botocore.exceptions import ClientError


"""
@dataclass
class S3ConnectionConfig:
    access_key: str
    secret_key: str
    bucket_name: str
    endpoint_url: str
    region_name: str = 'us-east-1'
"""

class S3Client:
    def __init__(self, net_params: S3Config):
        self._net_params = net_params
        self.session = get_session()

    @asynccontextmanager
    async def get_client(self):
        # Формируем конфигурацию для aiobotocore
        async with self.session.create_client(
            's3',
            region_name=self._net_params.region_name,
            endpoint_url=self._net_params.endpoint_url,
            aws_access_key_id=self._net_params.access_key,
            aws_secret_access_key=self._net_params.secret_key,
        ) as client:
            yield client

    def _calc_md5(self, file_path: str) -> tuple[str, str]:
        # MD5 файла, возврат - (hex-строка, base64-строка)
        hash_md5 = hashlib.md5()
        with open(file_path, 'rb') as file:
            for chunk in iter(lambda: file.read(4096), b''):
                hash_md5.update(chunk)

        hex_digest = hash_md5.hexdigest()
        base64_digest = base64.b64encode(hash_md5.digest()).decode('utf-8')

        return hex_digest, base64_digest

    async def check_exists(self,
                           key: str,
                           local_file_path: Optional[str] = None,
                           ) -> bool:
        # Проверяет наличие объекта в S3 через HEAD. Если передан local_file_path, сравнивает ETag с MD5 локального файла
        async with self.get_client(self) as client:
            try:
                response = await client.head_object(Bucket=self._net_params.bucket_name, Key=key)
                remote_etag = response.get('ETag', '').strip('"')

                if not local_file_path:
                    return True

                local_md5_hex, _ = self._calc_md5(local_file_path)

                # ВНИМАНИЕ: Если файл в S3 был загружен как Multipart, этот ETag не совпадет с MD5!
                # Для Multipart ETag выглядит как "md5-N".

                if '-' in remote_etag:
                    print(f'Warning! Remote object {key} is multipart-uploaded. ETag comparison is skipped.')
                    return True

                return remote_etag == local_md5_hex

            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    return False
                raise



    async def upload_object(self,
                          file_path: str,
                          object_name: str = None,):

        if object_name is None:
            object_name = os.path.basename(file_path)

        file_size = os.path.getsize(file_path)
        MULTIPART_THRESHOLD = 50 * 1024 * 1024

        async with self.get_client() as client:
            if file_size < MULTIPART_THRESHOLD:

                _, md5_base64 = self._calc_md5(file_path)

                with open(file_path, 'rb') as file_data:
                    await client.put_object(
                        Bucket=self._net_params.bucket_name,
                        Key=object_name,
                        Body=file_data,
                        ContentMD5=md5_base64,
                        Metadata={
                            'original_filename': os.path.basename(file_path),
                        }
                    )
                print(f'Uploaded object {object_name} with MD5 check.')
                return

            mp_create = await client.create_multipart_upload(
                Bucket=self._net_params.bucket_name,
                Key=object_name,
                Metadata={
                    'original_filename': os.path.basename(file_path),
                }
            )
            upload_id = mp_create['UploadId']
            parts = []

            try:
                CHUNK_SIZE = 10 * 1024 * 1024
                part_number = 1
                with open(file_path, 'rb') as file:
                    while True:
                        chunk = file.read(CHUNK_SIZE)
                        if not chunk:
                            break

                        chunk_md5 = hashlib.md5(chunk)
                        chunk_base64 = base64.b64encode(chunk_md5.digest()).decode('utf-8')

                        part_resp = await client.upload_part(
                            Bucket=self._net_params.bucket_name,
                            Key=object_name,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=chunk,
                            ContentMD5=chunk_base64,
                        )

                        parts.append({
                            'PartNumber': part_number,
                            'ETag': part_resp.get('ETag'),
                        })

                        print(f'Uploaded part {part_number} of {object_name}.')
                        part_number += 1

                await client.complete_multipart_upload(
                    Bucket=self._net_params.bucket_name,
                    Key=object_name,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts},
                )
                print(f'Multipart upload for {object_name} completed.')
            except Exception as e:
                print(f'Error occured when uploading object {object_name}: {e}.')
                await client.abort_multipart_upload(
                    Bucket=self._net_params.bucket_name,
                    Key=object_name,
                    UploadId=upload_id,
                )
                raise e

    async def download_object(self,
                              key: str,
                              dest_path: str):
        async with self.get_client() as client:
            try:
                response = await client.get_object(
                    Bucket=self._net_params.bucket_name,
                    Key=key,
                )
                async with response['Body'] as stream:
                    with open(dest_path, 'wb') as file:
                        async for chunk in stream:
                            file.write(chunk)
                print(f'Downloaded object {key} to {dest_path}.')
            except ClientError as e:
                print(f'Error occured when downloading object {key} to {dest_path}: {e}.')
                raise
