from infrastructure.common.keydb import KeyDbStatusStore as CommonKeyDbStatusStore

class KeyDbStatusStore(CommonKeyDbStatusStore):
    def __init__(self, redis_client):
        super().__init__(redis_client, prefix='ingest')
