

from services.general import MessageService
# from services.pipelines import PipelineWrapper
from services.streaming import SqlReadStreamer, SqlStreamTransformer

from utilities.import_tools import instansiate_engine
from utilities.postgres_queries import crypto_features_qry, tweet_features_qry, embeddings_matrix_qry

# configure services
msg_srvc = MessageService(print_level = 2)# if verbose else 1)
dbs_srvc = instansiate_engine('services.postgres', 'PostgresWriterService')
sql_strm = SqlReadStreamer(dbs_srvc,
                           crypto_features_qry(['coin_name',
                                                'price',
                                                'percent_change_24h',
                                                'time_idx'],
                                               'bitcoin'),
                           step=50)

cmc_data = [i for i in sql_strm()]
