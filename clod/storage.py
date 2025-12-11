import psycopg2 as ps
import json

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class DbConfig:
    host: str
    dbname: str
    user: str
    password: str
    port: int

    @staticmethod
    def from_dict(d: dict) -> "DbConfig":
        required = ('host', 'dbname', 'user', 'password', 'port')
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f'В конфиге БД отсутствуют поля: {', '.join(missing)}')
        port = d['port']
        try:
            port = int(port)
        except Exception:
            raise ValueError('Порт в конфиге должен вырыжаться в целочисленном типе данных')
        
        return DbConfig(
            host=str(d['host']),
            dbname=str(d['dbname']),
            user=str(d['user']),
            password=str(d['password']),
            port=port
        )
    

class DbConnection:
    def __init__(self, config: DbConfig):
        self.config = config

    def connect(self):
        return ps.connect(
            host=self.config.host,
            dbname=self.config.dbname,
            user=self.config.user,
            password=self.config.password,
            port=self.config.port
        )
    

class DbAgent:
    def __init__(self, config_path: Optional[str] = None,
                  config: Optional[DbConfig] = None):
        self._config: Optional[DbConfig] = config
        self._provider: Optional[DbConnection] = None


        if config_path is not None:
            self.config_path = Path(config_path)
        else:
            self.config_path = None

    
    def auth(self) -> bool:
        try:
            if self._config is None:
                if self.config_path is None:
                    raise RuntimeError("Ошибка агента БД: ни config_path, ни готовый config не были переданы")

                if not self.config_path.exists():
                    print(f"Ошибка агента БД: файл конфигурации {self.config_path} не найден")
                    return False
                
                raw = json.loads(self.config_path.read_text(encoding="utf-8"))
                self._config = DbConfig.from_dict(raw)

            self._provider = DbConnection(self._config)
            return True
        
        except Exception as e:
            print(f'Ошибка агента БД при загрузке конфигурации: {e}')
            return False
        

    def _connect(self):
        if self._provider is None:
            raise RuntimeError("Ошибка агента БД: Конфигурация не загружена, вызови auth().")

        return self._provider.connect()
    

    def _insert_load(self, load_id: str, operator: str) -> bool:
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO loads (load_id, source_system, operator)
                        VALUES (%s, %s, %s)
                        """,
                        (load_id, "python_importer", operator)
                    )
            return True

        except Exception as e:
            print(f"Ошибка агента БД при записи в loads: {e}")
            return False
        
    
    def execute(self, sql: str, params=None) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(sql, params)
            return True
        except Exception as e:
            print(f"Ошибка агента БД при исполнении SQL-запроса: {e}")
            return False
        
    
    def query(self, sql: str, params=None):
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        except Exception as e:
            print(f"Ошибка агента БД при query: {e}")
            return []