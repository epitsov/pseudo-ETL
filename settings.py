import os
import json

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

_settings = json.load(open(os.path.join(BASE_PATH, 'cfg.json')))

DATABASE_SETTINGS = {
    'db_host': _settings.get('db_host'),
    'db_port': _settings.get('db_port'),
    'db_username': _settings.get('db_username'),
    'db_password': _settings.get('db_password'),
    'db_name': _settings.get('db_name'),
}
