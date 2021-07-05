from sqlalchemy import create_engine, schema
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from .models import Message
from settings import DATABASE_SETTINGS

Base = declarative_base()


class PostgresUtil(object):
    SQLALCHEMY_CONNECTION_FORMAT = 'postgresql://{user}:{password}@{host}:{port}/{database}'

    def create(self):
        """Creates a database if it doesn't exist"""
        engine = self.get_sql_alchemy_engine()
        if not engine.dialect.has_schema(engine, "ETL"):
            engine.execute(schema.CreateSchema("ETL"))
        # Create tables
        Message.__table__.create(engine, checkfirst=True)

    def get_sql_alchemy_engine(self):
        db_settings = DATABASE_SETTINGS
        db_connection = self.SQLALCHEMY_CONNECTION_FORMAT.format(
            host=db_settings['db_host'],
            port=db_settings['db_port'],
            user=db_settings['db_username'],
            password=db_settings['db_password'],
            database=db_settings['db_name']
        )
        engine = create_engine(str(db_connection.strip()))
        return engine

    def get_session(self):
        engine = self.get_sql_alchemy_engine()
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = scoped_session(Session)
        return engine, session
