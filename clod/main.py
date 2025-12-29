import json
import logging
import os

from pathlib import Path
from itertools import count

from gateway import Checkpoint
from reproject import SRS
from insert import Insert
from fetch import Fetch
from records import Records
from cesium import CesiumImport


logger = logging.getLogger(__name__)

class Context:
    def __init__(self):
        self.files = []
        self.metadata = {}
        self.results = {}


class Step:
    def run(self, ctx: Context) -> bool:
        raise NotImplementedError
    

class StepCheckpoint(Step):
    def __init__(self, checkpoint):
        self.checkpoint = checkpoint

    
    def run(self, ctx: Context) -> bool:
        if not self.checkpoint.run():
            return False
        
        ctx.files = list(self.checkpoint.file_path)

        for fname, meta_path in self.checkpoint.metadata_json_path.items():
            with open(meta_path, encoding='utf-8') as f:
                ctx.metadata[fname] = json.load(f)

        return True
    

class StepReproject(Step):
    def __init__(self, in_srs, out_srs):
        self.in_srs = in_srs
        self.out_srs = out_srs

    def run(self, ctx: Context) -> bool:
        new_files = []
        failed_files = []

        for cloud_path in ctx.files:
            srs = SRS(
                cloud_path=cloud_path,
                in_srs=self.in_srs,
                out_srs=self.out_srs
            )

            out = srs.run()
            if not out:
                failed_files.append(cloud_path)
                continue

            new_files.append(out)

        if failed_files:
            print("Failed files:")
            for f in failed_files:
                print(f"  - {f}")

        ctx.files = new_files # заменяем старые пути на новые (репроецированные)
        return len(new_files) > 0 or len(ctx.files) == 0
    

class StepInsert(Step):
    def __init__(self, storage_cls, config_path):
        self.storage_cls = storage_cls
        self.config_path = config_path

    def run(self, ctx: Context) -> bool:
        for file_path in ctx.files:
            if not os.path.exists(file_path):
                print(f"Файла {file_path} не сущестует")
                return False

            storage = self.storage_cls(config_path=self.config_path)
            storage.cloud_path = str(file_path)

            if not storage.auth():
                print(f"Ошибка авторизации Storage для файла {file_path}")
                return False

            if not storage.run():
                print(f"Ошибка загрузки файла {file_path}")
                return False
        
        return True
    

class StepRecords(Step):
    def __init__(self, records_cls, config_path, records_table='loads'):
        self.records_cls = records_cls

        self.config_path = config_path
        self.records_table = records_table

        self.counter = count(0)

    def run(self, ctx: Context) -> bool:
        
        if not self.records_cls.run():
            print('Ошибка чтения записей из БД')
            return False

        #TODO: мб пригодится в будущем работа с наследниками Records?
        # Подготовка для цикла по "user" со своими специфичными вовзратами records
        try:
            ctx.results["records"][next(self.counter)] = self.records_cls.records_df
        except Exception as e:
            print(f'Ошибка сохранения records в контекст Step: {e}')
            return False
        return True
        

class StepFetch(Step):
    def __init__(self, config_path, fetch_cls, load_id, save_path):
        self.config_path = config_path
        self.fetch_cls = fetch_cls
        self.load_id = load_id
        self.save_path = save_path

    
    def run(self, ctx: Context) -> bool:
        for file_path in ctx.files:
            fetch = self.fetch_cls(
                config_path=self.config_path,
                load_id=self.load_id,
                cloud_path=file_path,
                save_path=self.save_path
            )
        
            if not fetch.run():
                return False
        return True


class StepCesiumTiles(Step):
    def __init__(self, cesium_cls):
        self.cesium_cls = cesium_cls

    def run(self, ctx: Context) -> bool:
        for file_path in ctx.files:
            cesium = self.cesium_cls(
                cloud_path=file_path
            )
            if not cesium.run():
                return False
        return True


class Orchest:
    def __init__(self, steps):
        self.steps = steps
    
    def run(self):
        ctx = Context()

        for step in self.steps:
            if not step.run(ctx):
                return False
        
        return True
    

orchest = Orchest([
    StepCheckpoint(checkpoint=Checkpoint()),
    StepReproject(in_srs='EPSG:4490', out_srs='EPSG:4326')
])

 
# StepCheckpoint(checkpoint=Checkpoint()),
# StepReproject(in_srs='EPSG:4326', out_srs='EPSG:4978'),

cesium = Orchest([
    StepCheckpoint(checkpoint=Checkpoint()),
    StepReproject(in_srs='EPSG:4326', out_srs='EPSG:4978'),
])

cesium.run()