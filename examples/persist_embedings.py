# Run as: python -i examples/persist_embedings.py <path-to-csv-> --table-suffix glove25 --create-table

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("filepath",  help="input csv file path, IN THE DB HOST MACHINE!!!")
parser.add_argument("--table-name",  type=str, help='table suffix')
opts = parser.parse_args()

import pandas as pd
from asyncpg.exceptions import DuplicateTableError

from services.postgres import PostgresReaderService
from services.postgres_queries import column_names, create_embs_table, copy_csv_to_table

# db service
db = PostgresReaderService()

table_name = opts.table_name
column_names = ['word'] + ['embd_%s'%i for i in range(25)]

# create table, if it does not exist
column_types = ['text'] + ['numeric(8,6)' for i in range (25)]

create_qry = create_embs_table(table_name, column_names, column_types)
try:
    db.query(create_qry)
    print('Creatied table "%s" '%table_name)
except DuplicateTableError:
    print('Table "%s" already exists. Skiping '%table_name)


# copy csv to table
print('Trying to import %s '%opts.filepath)
print('Make sure the parth refers to the host db machine.')

copy = copy_csv_to_table(table_name, column_names, opts.filepath)

db.query(copy)


