

## Before you start edit cfg.json


# How to use:

etl.source('file', add_all=True).sink('console').run() -> prints all the data from the file

etl.source('file', add_all=True).sink('db').run() -> adds all the data from the file to the database


etl.source('file').sink('console').run() -> prints the first json from the file

etl.source('file').sink('console').run() -> prints the second json from the file

If you try to print another json from the file after reading the last one you will get IndexError('You have read all the elements in the file OR the file is empty.')


etl.source('file').sink('db').run() -> adds the first json to the file database

etl.source('file').sink('db').run() -> adds the second json to the file database

If you try to save another json to the db from the file after saving the last one you will get IndexError('You have read all the elements in the file OR the file is empty.')


etl.source('simulation').sink('db').run() -> adds a random combination of json file to the database

etl.source('simulation').sink('cosole').run() -> prints a random combination of json file



etl.source('db').sink('db').run() -> ValueError('you already have this entry in the database')


etl.source('db').sink('console').run() -> prints the first row from the database

etl.source('db').sink('console').run() -> prints the second row from the database

If you try to print another row after you have read the last row in the db you will get AttributeError('You have read all the elements in the database OR the database is empty.')
