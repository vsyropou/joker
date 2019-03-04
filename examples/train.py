

from services.general import MessageService
# from services.pipelines import PipelineWrapper
from services.streaming import SqlReadStreamer #, SqlStreamTransformer

from utilities.import_tools import instansiate_engine
from utilities.postgres_queries import crypto_features_qry, tweet_features_qry, tweet_visible_features_qry, embeddings_matrix_qry

import tensorflow as tf
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

# parse configuration
batch_size = -1

# configure services
msg_srvc = MessageService(print_level = 2)# if verbose else 1)
dbs_srvc = instansiate_engine('services.postgres', 'PostgresWriterService')

# data streams
coin_cols = ['price as price_zero',
             'price as price_one',
             'percent_change_24h',
             'time_idx']

twt_cols = ['embeding_keys_array',
            'favorite_count',
            'retweet_count',
            'user_followers_count',
            'user_friends_count',
            'user_favourites_count',
            'tft.time_idx']

twt_ftrs_cols = ['favorite_count',
                 'retweet_count',
                 'user_followers_count',
                 'user_friends_count',
                 'user_favourites_count']

btc_stream = SqlReadStreamer(dbs_srvc,
                             crypto_features_qry(coin_cols, 'bitcoin'),
                             step=batch_size,
                             records_format='tuple')

ltc_stream = SqlReadStreamer(dbs_srvc,
                             crypto_features_qry(coin_cols, 'litecoin'),
                             step=batch_size,
                             records_format='tuple')

twt_stream = SqlReadStreamer(dbs_srvc,
                             tweet_visible_features_qry(twt_cols),
                             step=batch_size,
                             records_format='list')

emb_stream = SqlReadStreamer(dbs_srvc,
                             embeddings_matrix_qry(25, 'embedings_glove25'),
                             step=batch_size,
                             records_format='nparray')

# coin data
btc_data = pd.DataFrame([i for i in btc_stream()], columns=coin_cols)
ltc_data = pd.DataFrame([i for i in ltc_stream()], columns=coin_cols)

btc_data.index = btc_data['time_idx']
ltc_data.index = ltc_data['time_idx']

# tweet data
twt_data = pd.DataFrame([i for i in twt_stream()], columns=twt_cols)

twt_text = twt_data[['embeding_keys_array','tft.time_idx']].groupby('tft.time_idx').agg(lambda tweets: [emb  for t in tweets for emb in t] )

twt_ftrs = twt_data[twt_ftrs_cols+['tft.time_idx']].groupby('tft.time_idx').agg(np.mean)

twt_ftrs = pd.DataFrame(StandardScaler().fit_transform(twt_ftrs), columns=twt_ftrs_cols, index=twt_ftrs.index)

# embedings
emb_table = pd.DataFrame([i for i in emb_stream()], columns=['idx','word', 'embeddings'])

pca = PCA(n_components=1)
emb_table_reduced = pd.DataFrame(pca.fit_transform([*emb_table['embeddings']]))

# embedded tweets
embedded_tweets = twt_text['embeding_keys_array'].apply(lambda tokn_twt: [emb_table_reduced.loc[idx][0] for idx in tokn_twt])

#embedded_tweets = pd.read_csv('temp/embeded_tweets.csv')

assert False

tweet_length = twt_text['embeding_keys_array'].apply(lambda x: len(x)).max()
from keras.layers import Input, Embedding, LSTM, Dense
from keras.models import Model

main_input = Input(shape=(100,), dtype='int32', name='main_input')





# TODO: get tweets, and two coin streams and a full static embedigns matrix
# TODO: read into tf.Dataset and tf.SqlDataset (only proecess with tensoflow or in the db)
# TODO: perhaps write a tensorflow transoformer to perform the sessions data parsing confs and shit. plus a pipeline component for training (fit method of the pipline wrapper object)

