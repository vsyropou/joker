

from services.general import MessageService
# from services.pipelines import PipelineWrapper
from services.streaming import SqlReadStreamer #, SqlStreamTransformer

from utilities.import_tools import instansiate_engine
from utilities.postgres_queries import crypto_features_qry, tweet_features_qry, embeddings_matrix_qry

import tensorflow as tf

# parse configuration
batch_size = 50

# configure services
msg_srvc = MessageService(print_level = 2)# if verbose else 1)
dbs_srvc = instansiate_engine('services.postgres', 'PostgresWriterService')

# data streams 

btc_stream = SqlReadStreamer(dbs_srvc,
                             crypto_features_qry(['coin_name',
                                                  'price as price_zero',
                                                  'price as price_one'
                                                  # 'percent_change_24h',
                                                  # 'time_idx'
                             ],
                                                 'bitcoin'),
                             step=batch_size,
                             records_format='tuple')

btc_data = tf.data.Dataset.from_generator(btc_stream,
                                          # ((tf.string, tf.float16,tf.float16),)*batch_size
                                          ((tf.string, tf.float16,tf.float16),)*batch_size
)

                                         
val = btc_data.make_one_shot_iterator().get_next()

with tf.Session() as sn:
    print(sn.run(val))
    print(sn.run(val))
#    assert False
    
    # btc_data = [i for i in strm]



# TODO: get tweets, and two coin streams and a full static embedigns matrix
# TODO: read into tf.Dataset and tf.SqlDataset (only proecess with tensoflow or in the db)
# TODO: perhaps write a tensorflow transoformer to perform the sessions data parsing confs and shit. plus a pipeline component for training (fit method of the pipline wrapper object)

