import json
import os
from settings import BASE_PATH




class Reader:
    data: json
    """Reads a row from the file and saves it. Use get_data to get the data"""

    def __init__(self):
        self.data = json.load(open(os.path.join(BASE_PATH, 'data.json')))

    def get_whole_data(self):
        return self.data

    def get_data(self, i):
        return self.data[i]
