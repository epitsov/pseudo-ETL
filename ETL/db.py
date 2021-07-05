import json
from datetime import datetime

from sqlalchemy.orm import sessionmaker

from .models import Message
from .utils import PostgresUtil

db = PostgresUtil()


class DatabaseReader:
    """Reads a row from the database and saves it. Use return_to_json to get the data in json format"""
    id: str
    db_id: int
    value: float
    timestamp: datetime
    session: sessionmaker

    def __init__(self, db_id):
        Session = sessionmaker(bind=db.get_sql_alchemy_engine())
        self.session = Session()
        self.db_id = db_id
        self._get_data()

    def return_to_json(self):
        values_dict = {
            'id': self.id,
            'value': self.value,
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        }
        return json.dumps(values_dict)

    def _get_data(self):
        message_obj = self.session.query(Message).filter(Message.id >= self.db_id).order_by(Message.id.asc())\
            .first()
        self.session.close()
        setattr(self, 'db_id', message_obj.id)
        setattr(self, 'id', message_obj.generated_id)
        setattr(self, 'value', message_obj.value)
        setattr(self, 'timestamp', message_obj.timestamp)

