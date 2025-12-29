import json
import pdal

from storage import DbAgent

from typing import Optional
from pathlib import Path


class Fetch(DbAgent):
    # TODO: нужно будет реализовать журнал записей и чтений БД по пользователям
    def __init__(self, config_path, load_id, cloud_path: str = '', save_path: str = 'fetched_clouds'):
        
        super().__init__(config_path=config_path)

        self.cloud_path = cloud_path
        self.load_id = load_id

        self.save_path = Path(save_path)
        self.clouds_table: Optional[str] = "clouds"
        self.pgsql_where: Optional[str] = ""

    
    def _create_load(self):
        return self._insert_load(self.load_id, "DOWNLOAD")


    def _fetch_cloud(self):
        save_path = self.save_path
        if not save_path.exists():
            print(f'Директория для сохранения {save_path} не найдена')
            return None
        
        cfg = self._config
        conn_str = (
            f"host='{cfg.host}' dbname='{cfg.dbname}' user='{cfg.user}' "
            f"password='{cfg.password}' port='{cfg.port}'"
        )

        pipeline_json = {
            "pipeline": [
                {
                    "type": "readers.pgpointcloud",
                    "connection": conn_str,
                    "table": self.clouds_table,
                    "column": "pa",
                    "where": rf"load_id = '{self.load_id}'"
                },
                {
                    "type": "writers.las",
                    "filename": f"{str(self.save_path)}/{self.cloud_path}",
                    "threads": 16
                }
            ]
        }

        try:
            pipe = pdal.Pipeline(json.dumps(pipeline_json))
            pipe.execute()
            return True
            #TODO: Взять numpy массив и возвращать его
        except Exception as e:
            print(f'Ошибка выгрузки облака из таблицы {self.clouds_table}: {e}')
            return False
    

    def run(self) -> bool:
        if not self.auth():
            return False

        if not self._create_load():
            return False
        
        if not self._fetch_cloud():
            return False
        
        return True