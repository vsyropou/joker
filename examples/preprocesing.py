
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("conf_file", help="provide configuration file")
parser.add_argument("--input-tweets", help="tweets csv file path")
parser.add_argument("--verbose", action='store_true', help="maximum output")
opts = parser.parse_args()

import json
import numpy as np
from pandas import read_csv

from services.general import MessageService
from services.pipelines import PreProcessingPipelineWrapper
from services.postgres import PostgresReaderService
from utilities.postgres_queries import all_tweets_qry


limit = 5 # for developing

# get some data
data_db_svc  = PostgresReaderService()
query_result = data_db_svc.query(all_tweets_qry(['id','text', 'lang']))

tweets = list(map(lambda tpl: tpl[1], query_result))[:limit]
twkeys = list(map(lambda tpl: tpl[0], query_result))[:limit]
twlang = list(map(lambda tpl: tpl[2], query_result))[:limit]


# configure pipeline
conf = json.load(open(opts.conf_file,'r'))

conf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_sentence_ids'] = twkeys
conf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_sentence_lang'] = twlang


# initialize services
msg_svc = MessageService(print_level = 2 if opts.verbose else 1)

pipeline = PreProcessingPipelineWrapper(conf)


out = list(pipeline.transform(list(tweets)))

