import datetime as dt
import json

from sqlalchemy import Column, Integer, String, Sequence, DateTime, Float, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Message(Base):
    """Creates the table columns"""
    __tablename__ = 'messages'
    __table_args__ = (UniqueConstraint('generated_id', 'value', name='uix_1'), {"schema": "ETL"})
    id = Column(Integer,
                Sequence(name='id_seq', schema='ETL', start=1, increment=1),
                primary_key=True,
                unique=True,
                nullable=False)
    generated_id = Column(String, unique=False, nullable=False)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False, default=dt.datetime.utcnow)

    def __init__(self, generated_id, value, timestamp):
        self.generated_id = generated_id
        self.value = value
        self.timestamp = timestamp

    def to_dict(self):
        return {c.name: str(getattr(self, c.name)) for c in self.__table__.columns}

    def to_json(self):
        return json.dumps(self.to_dict())
