from infrastructure.db import get_session
from infrastructure.orm_models import Base  # важно: тут должен быть Base от declarative_base
from sqlalchemy import text

def main():
    db = get_session()
    try:
        # снести core
        db.execute(text("DROP SCHEMA IF EXISTS core CASCADE"))
        db.execute(text("CREATE SCHEMA core"))
        db.commit()

        # создать все таблицы из моделей
        Base.metadata.create_all(bind=db.get_bind())
        print("OK: schema core recreated and tables created")
    finally:
        db.close()

if __name__ == "__main__":
    main()