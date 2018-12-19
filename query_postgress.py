
import utilities
from utilities import postgress

db = postgress.TweetsDbConnector()

data = db.query('''SELECT * FROM tweets''')

# v =  [v for v in data[0].values()]
