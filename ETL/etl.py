import json

from sqlalchemy import exc
from sqlalchemy.orm import sessionmaker

from .simulator import Simulation
from .db import DatabaseReader
from .models import Message
from .reader import Reader
from .utils import PostgresUtil


class ETL:
    """Creates an etl object.
    source- downloads data from the source that you have specified
    sink- set the source in which the data will be displayed-
          either in the console or uploads it to the database
    run- performs the operations
    """
    i: int
    j: int
    data: dict
    db: PostgresUtil
    to_print: bool

    def __init__(self):
        self.i = 0
        self.j = 1
        self.data = dict
        self.to_print = True
        self.db = PostgresUtil()
        self.db.create()
        self.kwags_argument = False

    def source(self, source_args: str, **kwargs):
        if source_args.lower() == 'file' and kwargs.get('add_all'):
            setattr(self, 'whole_data', Reader().get_whole_data())
            setattr(self,'kwags_argument', True)

        elif source_args.lower() == 'file':
            try:
                self.data = json.loads(Reader().get_data(self.i))
                self.i += 1
            except IndexError:
                raise IndexError('You have read all the elements in the file OR the file is empty.')
        elif source_args.lower() == 'simulation':
            self.data = json.loads(Simulation().return_to_json())

        elif source_args.lower() in ['database', 'db']:
            try:
                db_data = DatabaseReader(self.j)
                self.data = json.loads(db_data.return_to_json())
                self.j = int(db_data.db_id) + 1
            except AttributeError:
                raise AttributeError('You have read all the elements in the database OR the database is empty.')

        else:
            raise NameError('NOT A VALID source_args- you can accept data from Simulation, File or Database')
        return self

    def sink(self, sink_args: str):
        if sink_args.lower() == 'console':
            setattr(self, 'to_print', True)
        elif sink_args.lower() in ['db', 'database']:
            setattr(self, 'to_print', False)
        else:
            raise NameError('NOT A VALID sink_args - you can send data to the console or database')
        return self

    def run(self):
        if self.kwags_argument:
            if self.to_print:
                for i in self.whole_data:
                    print(i)
            else:
                self._add_all_to_db()
        else:
            if self.to_print:
                print(self.data)
            else:
                self._add_to_db()

    def _add_to_db(self):
        if self.data != {}:
            message = Message(generated_id=self.data['id'], value=self.data['value'], timestamp=self.data['timestamp'])
        else:
            raise ValueError('Cannot send an upload an empty json to the db')
        try:
            Session = sessionmaker(bind=self.db.get_sql_alchemy_engine())
            session = Session()
            session.add(message)
            session.commit()
        except exc.IntegrityError:
            raise ValueError('you already have this entry in the database')

    def _add_all_to_db(self):
        for i in self.whole_data:
            data = json.loads(i)
            if self.data != {}:
                message = Message(generated_id=data['id'], value=data['value'],
                                  timestamp=data['timestamp'])
            else:
                raise ValueError('Cannot send an upload an empty json to the db')
            try:
                Session = sessionmaker(bind=self.db.get_sql_alchemy_engine())
                session = Session()
                session.add(message)
                session.commit()
            except exc.IntegrityError:
                raise ValueError('you already have this entry in the database')