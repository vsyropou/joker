
import pandas as pd
from services.postgres import PostgresReaderService
from services.postgres_queries import column_names

# embedings_file = '/data/glove.twitter.27B/glove.twitter.27B.25d.txt'
embedings_file = '/data/glove.twitter.27B/test.csv'
delimeter = ' '

db = PostgresReaderService()

column_names = ['word'] + ['embd_%s'%i for i in range(25)]
# column_types = ['text'] + ['real' for i in range (25)]
column_types = ['text'] + ['numeric(8,6)' for i in range (25)]

# TODO: parameterize as lambda and put in postrgrss_queries
# create query 
create = "CREATE TABLE embedingss "
create += '('
create += ' id SERIAL PRIMARY KEY, '
create += ', '.join([' '.join([nam,typ,'NOT NULL ']) for nam, typ in zip(column_names, column_types)])
create += ' );'
# copy query
copy = "COPY embedingss (%s) FROM '%s' DELIMITER ',' CSV"%(', '.join(column_names),
                                                           embedings_file)

# db.query(create)

#db.query(copy)
