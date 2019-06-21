

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


# configure network
n_cnn_filters = 64
embedings_kernel_size = 4

time_sequence_length = 2
batch_length = 5

text_sequence_length = 100
embeddings_vector_length = 2



# data streams
coin_cols = ['price as price_zero',
             'lead(price, 1, NULL) OVER (PARTITION BY coin_id ORDER BY insertion_time) as price_one',
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
                             step=-1,
                             records_format='tuple')

ltc_stream = SqlReadStreamer(dbs_srvc,
                             crypto_features_qry(coin_cols, 'litecoin'),
                             step=-1,
                             records_format='tuple')

twt_stream = SqlReadStreamer(dbs_srvc,
                             tweet_visible_features_qry(twt_cols),
                             step=-1,
                             records_format='list')

emb_stream = SqlReadStreamer(dbs_srvc,
                             embeddings_matrix_qry(25, 'embedings_glove25'),
                             step=-1,
                             records_format='nparray')

# coin data
btc_data = pd.DataFrame([i for i in btc_stream()], columns=coin_cols)

ltc_data = pd.DataFrame([i for i in ltc_stream()], columns=coin_cols)

btc_data.index = btc_data['time_idx']
ltc_data.index = ltc_data['time_idx']


# # tweet data
# twt_data = pd.DataFrame([i for i in twt_stream()], columns=twt_cols)

# twt_ftrs = twt_data[twt_ftrs_cols+['tft.time_idx']].groupby('tft.time_idx').agg(np.mean)

# twt_ftrs = pd.DataFrame(StandardScaler().fit_transform(twt_ftrs), columns=twt_ftrs_cols, index=twt_ftrs.index)


# # embedded tweets
# emb_table = pd.DataFrame([i for i in emb_stream()], columns=['idx','word', 'embeddings'])

# pca = PCA(n_components=embeddings_vector_length)
# emb_table_reduced = pd.DataFrame(pca.fit_transform([*emb_table['embeddings']]))

# #   ugly and not readable as fuck
# agg_func = lambda tweets: [np.array([emb_table_reduced.loc[tkn][:].values for tkn in t ]) for t in tweets]
# embedded_tweets = twt_data[['embeding_keys_array','tft.time_idx']].groupby('tft.time_idx').agg(agg_func) # group tweets by coin time index and embeed text
# embedded_tweets = np.array([np.array(train[0]) for train in embedded_tweets.values ]) # simply convert nested lists to np.arrays,  

# np.save('temp/embeded_tweets_length_%s.npy'%embeddings_vector_length, embedded_tweets)


print('read embedded tweets')
embedded_tweets = np.load('temp/embeded_tweets_length_%s.npy'%embeddings_vector_length)


import keras
from keras.layers import ConvLSTM2D, Conv1D, AveragePooling1D, Dense
from keras.models import Model
from keras import backend

input_tensor = tf.placeholder(name='input_tensor',
                              dtype=tf.float32,
                              shape=(batch_length, time_sequence_length, None, None, embeddings_vector_length))

conv_lstm_layer = ConvLSTM2D(n_cnn_filters, # filters
                             embedings_kernel_size, # kernel_size,
#                             input_shape = (batch_length, time_sequence_length, None, None, embeddings_vector_length),
                             strides=1,
                             padding='valid', #'causal',
                             data_format='channels_last',
                             dilation_rate=1,
                             activation='relu', # maybe softplus
                             recurrent_activation='relu', # maybe softplus
                             return_sequences=True,
                             stateful=True,
                             recurrent_dropout=0.8,
                             dropout=0.8,
                             name = 'conv_lstm'
                       )

pool_layer_1 = AveragePooling1D(pool_size=4,
                                padding='valid',
                                data_format='channels_last',
                                name = 'pool_layer_1'
                                )


num_batches = int(len(embedded_tweets) / batch_length)

data_func = lambda batch, time: embedded_tweets[:batch*time].flatten().reshape(batch,time)


tst_data = data_func(batch_length,time_sequence_length)



# model = Model(inputs=[main_input, auxiliary_input], outputs=[main_output, auxiliary_output])

# trainable_count = int(
#     np.sum([K.count_params(p) for p in set(model.trainable_weights)]))
# non_trainable_count = int(
#     np.sum([K.count_params(p) for p in set(model.non_trainable_weights)]))

# print('Total params: {:,}'.format(trainable_count + non_trainable_count))
# print('Trainable params: {:,}'.format(trainable_count))
# print('Non-trainable params: {:,}'.format(non_trainable_count))



init = tf.global_variables_initializer()
with tf.Session() as sn:
    init.run()
    tst = sn.run(conv_lstm_layer, feed_dict={input_tensor:tst_data})

    
    # inp = test_input.eval() 
    # cnv = conv_out.eval()
    # pol = pool_out.eval()
    

# TODO: pen and paper exercise for one example (this is very important)
# TODO: Visualize the fucking network
# REMEMBER: the chanells dimension can by ascociated with the embedings dimmension
