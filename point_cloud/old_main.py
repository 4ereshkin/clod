import json
from checkpoint import Checkpoint
from point_cloud.insert import Insert
from point_cloud.fetch import Fetch
from point_cloud.reproject import SRS

class Orchest:
    def __init__(self, checkpoint: Checkpoint, storage: Insert,  srs: SRS):
        self.checkpoint = checkpoint
        self.srs = srs
        self.storage = storage
        self.metadata = {}


    def _load_metadata(self) -> bool:
        for fname, meta_path in self.checkpoint.metadata_json_path.items():
            try:
                with open(meta_path, encoding="utf-8") as f:
                    self.metadata[fname] = json.load(f)
            except Exception as e:
                print(f"Ошибка чтения метаданных {meta_path}: {e}")
                return False
        return True
    

    def _process_cloud(self, file_path) -> bool:
        self.storage.cloud_path = file_path
        if not self.storage.run():
            print(f'Ошибка загрузки облака для хранения')
            return False
        return True


    def run(self):
        if not self.checkpoint.run():
            print("Ошибка чекпоинта.")
            return False
        
        

        print(f'Файлы: {self.checkpoint.file_path}')
        print(f'Метаданные: {self.checkpoint.metadata_json_path.values}')

        for file in self.checkpoint.metadata_json_path:
            with open(self.checkpoint.metadata_json_path[file], encoding='utf-8') as f:
                self.metadata[file] = json.load(f)


        '''self.srs.cloud_path = self.checkpoint.file_path
        self.srs.cloud_metadata = self.metadata

        if self.srs.run():
            print(f'Репроекция в EPSG:4326 завершена: {self.srs.reprojected_cloud_path}')
            return True
        else:
            print(f'Репроекция не удалась.')
            return False'''
        
        for file in self.checkpoint.file_path:
            
            loading = Insert()

            loading.cloud_path = file

            if loading.auth():
                loading.run()
            else:
                print(f'Storage error with file: {file}')
                return False
            
            '''answ = str(input('Хотите выгрузить облако? Y/N'))
            if answ == 'Y':
                load_id = str(input('Введите load_id: '))
                if load_id:
                    fetch = Fetch(load_id=load_id, cloud_path=file,
                                   save_path="copc_files")
                    fetch.run
                    
                else:
                    print('bb')
                    return False

            else:
                print('BB')
                return False'''

    
        
        
        
         
checkpoint = Checkpoint()
srs = SRS()
storage = Insert()
orchest = Orchest(checkpoint=checkpoint, storage=storage, srs=srs)
orchest.run()
