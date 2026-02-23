from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from legacy_env_vars import settings


engine = create_engine(settings.pg_dsn, pool_pre_ping=True)
Session = sessionmaker(bind=engine)


def get_session():
    return Session()