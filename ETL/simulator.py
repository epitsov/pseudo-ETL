import random
import string
from datetime import datetime
import json

from faker import Faker


class Simulation:
    """Creates a random combination of id, value and timestamp"""

    id: str
    value: float
    timestamp: datetime

    def __init__(self):
        self.id = self._id_generator(4)
        self.value = self._value_generator()
        self.timestamp = self._date_generator()

    def __repr__(self):
        values_dict = {
            'id': self.id,
            'value': self.value,
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        }
        return json.dumps(values_dict)

    def return_to_json(self):
        values_dict = {
            'id': self.id,
            'value': self.value,
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        }
        return json.dumps(values_dict)

    @staticmethod
    def _id_generator(size):
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choice(chars) for _ in range(size))

    @staticmethod
    def _value_generator():
        return round(random.uniform(0, 100), 3)

    @staticmethod
    def _date_generator():
        fake = Faker()
        return fake.date_time_between('now', '+10y')
