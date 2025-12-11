import json
import pdal
import uuid

from psycopg2 import sql
from clod.storage import DbAgent
from urllib.parse import quote_plus

from pathlib import Path
from datetime import datetime
from typing import Optional


class Insert(DbAgent):
    def __init__(self, config_path: Optional[str] = 'clod/db.json'):
        super().__init__(config_path=config_path)

        self.cloud_path: str = ''
        self._load_id: Optional[str] = None

        self._staging_table: Optional[str] = None


    def _get_source_name(self) -> str:
        return Path(self.cloud_path).stem
    

    def _create_load(self):
        self._load_id = str(uuid.uuid4())
        return self._insert_load(self._load_id, "LOAD")
    

    def _make_staging_name(self) -> str:
        assert self._load_id is not None
        safe_id = self._load_id.replace('-', '')
        return f"clouds_staging_{safe_id}"
        
    
    def _create_staging_table(self) -> bool:
        if self._load_id is None:
            raise RuntimeError("Ошибка создания staging таблицы: load_id не был сгенерирован.")

        self._staging_table = self._make_staging_name()

        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {}.{} (
                        patch_id SERIAL PRIMARY KEY,
                        pa pcpatch NOT NULL);
                            """).format(sql.Identifier('public'), sql.Identifier(self._staging_table))
                            )
            print("STAGING CREATED:", self._staging_table)
            return True

        except Exception as e:
            print(f"Ошибка создания staging-таблицы: {e}")
            return False
        

    def _drop_staging(self):
        if not self._staging_table:
            return
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    sql.SQL('DROP TABLE IF EXISTS {}.{};').format(
                        sql.Identifier('public'), sql.Identifier(self._staging_table)
                        )
                    )

        except Exception as e:
            print(f'Ошибка удаления промежуточной (staging) таблицы {e}')


    def _write_to_staging(self) -> Optional[dict]:
        source_path = Path(self.cloud_path)
        if not source_path.exists():
            print(f'Файл {source_path} не найден')
            return None
        
        if self._staging_table is None:
            print("Промежуточная (staging) таблица не создана")
            return None

        cfg = self._config
        
        if cfg is None:
            print("Отсутствует конфигурация БД для подключения")
            return None

        conn_str = (
            f"host='{cfg.host}' dbname='{cfg.dbname}' user='{cfg.user}' "
            f"password='{cfg.password}' port='{cfg.port}'"
        )

        pipeline_json = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": str(self.cloud_path)
                },
                {
                    "type": "filters.chipper",
                    "capacity": 600
                },
                {
                    "type": "writers.pgpointcloud",
                    "connection": conn_str,
                    "table": self._staging_table,
                    "column": "pa",
                    "compression": "laz"
                }
            ]
        }

        try:
            pipe = pdal.Pipeline(json.dumps(pipeline_json))
            count = pipe.execute()

            if count == 0:
                return None
            
            meta = pipe.metadata
            
            return meta
            
        except Exception as e:
            print(f'Ошибка записи в staging: {e}')
            print(f'Тип ошибки: {type(e).__name__}')
            import traceback
            traceback.print_exc()  # полный stack trace
            if hasattr(pipe, 'log'):
                print(f'PDAL log: {pipe.log()}')
            return None
        
    
    def _merge_staging_to_clouds(self, source_name: str) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    sql.SQL("""
                        INSERT INTO clouds (pa, source_name, load_id, loaded_at, service)
                        SELECT pa, %s, %s, CURRENT_TIMESTAMP, 'python_importer'
                        FROM {}.{};"""
                            ).format(sql.Identifier('public'), sql.Identifier(self._staging_table)),
                    (source_name, self._load_id))
            return True
        
        except Exception as e:
            print(f'Ошибка переноса данных из staging в clouds: {e}')
            return False


    def _sanitize(self, obj):
        if isinstance(obj, dict):
            return {k: self._sanitize(v) for k, v in obj.items()}
        
        if isinstance(obj, list):
            return [self._sanitize(v) for v in obj]
        
        if callable(obj):
            return str(obj)
        
        return obj


    def _save_metadata_to_db(self, meta: dict) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("""
                            INSERT INTO metadata (metadata, load_id)
                            VALUES (%s::jsonb, %s)
                            """, (json.dumps(meta, ensure_ascii=False), self._load_id))
            return True
        
        except Exception as e:
            print(f'Ошибка записи метаданных: {e}')
            return False
        
    
    def _save_metadata_file(self, source_name: str, meta_dict: dict) -> bool:
        try:
            storage_dir = Path('activities_data/storage_metadata')
            storage_dir.mkdir(exist_ok=True)

            source_dir = storage_dir / source_name
            source_dir.mkdir(exist_ok=True)

            metadata_content = {
                "origin_file": str(self.cloud_path),
                "source_name": source_name,
                "loaded_at": datetime.now().isoformat(),
                "patching_metadata": meta_dict
            }

            metadata_path = source_dir / f'{source_name}_metadata.json'

            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata_content, f, ensure_ascii=False, indent=4)
            return True
        except Exception as e:
            print(f"Ошибка сохранения локальных метаданных: {e}")
            return False


    def run(self) -> bool:
        if not self.auth():
            return False

        if not self._create_load():
            return False

        if not self._create_staging_table():
            print('Ошибка создания')
            return False

        meta = self._write_to_staging()
        if meta is None:
            print('Ошибка meta')
            self._drop_staging()
            return False
        
        meta = self._sanitize(meta)
        source_name = self._get_source_name()

        if not self._merge_staging_to_clouds(source_name=source_name):
            self._drop_staging()
            return False
        
        self._save_metadata_to_db(meta)
        self._save_metadata_file(source_name=source_name, meta_dict=meta)

        self._drop_staging()
        return True