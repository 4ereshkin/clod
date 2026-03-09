from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from application.common.config import get_settings



engine = create_engine(get_settings().postgres.dsn, pool_pre_ping=True)
Session = sessionmaker(bind=engine)


def get_session():
    return Session()