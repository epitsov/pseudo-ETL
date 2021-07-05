from ETL.etl import ETL

e = ETL()



e.source('db').sink('db').run()











