
import pandas as pd
from services.postgres import PostgresReaderService
from services.postgres_queries import column_names

db = PostgresReaderService()

column_names = list(map(lambda r: list(r.values())[0],
                        db.query(column_names('tweets'))))


for sufix, query, columns in zip(['text', 'full'],
                                 ['''SELECT text FROM tweets''', '''SELECT *FROM tweets'''],
                                 [['text'], column_names]):
    
    response = db.query(query)
    dframe   = pd.DataFrame(response, columns=columns)

    dframe.to_csv('tweets_%s.csv'%sufix)
    
 
