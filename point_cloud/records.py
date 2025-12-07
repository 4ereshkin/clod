import psycopg2 as ps
from psycopg2 import sql

from pandas import DataFrame
from storage import DbAgent


class Records(DbAgent):
    def __init__(self, config_path, records_table = "loads"):
        
        super().__init__(config_path=config_path)
        
        self.records_df = None
        self.records_table = records_table

    '''def _create_load(self):
        return self._insert_load(load_id=self.load_id, operator="READ")'''
    
    def records_rows_json(self, rows: list, columns=['load_id', 'source_name',
                                          'created_at', 'source_system',
                                            'operator']):
        if len(rows) == 0:
            print('Ошибка чтения records: пустой результат')
            return False
        try:
            self.records_df = DataFrame(rows, columns=columns)
            print(self.records_df.head(5))
            return True
        except Exception as e:
            print(f'Ошибка преобразования records в DataFrame: {e}')
            return False
    
    def _read_records(self) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    sql.SQL("""
                        CREATE TEMPORARY TABLE temp_records AS
                        SELECT DISTINCT
                            c.load_id,
                            c.source_name,
                            l.created_at,
                            l.source_system,
                            l.operator
                        FROM {} c
                        JOIN {} l ON c.load_id = l.load_id;
                    """).format(sql.Identifier('clouds'),
                      sql.Identifier('loads'))
                )
                cur.execute("""SELECT * FROM temp_records""")
                rows = cur.fetchall()

                if not self.records_rows_json(rows=rows):
                    return False
                return True
            
        except Exception as e:
            print(f'Ошибка чтения records: {e}')
            return False

    def run(self) -> bool:
        '''if not self._create_load():
            return False'''
        
        if not self.auth():
            return False

        if not self._read_records():
            return False

        return 