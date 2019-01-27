
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("conf_file", help="provide configuration file")
parser.add_argument("--input-tweets", help="tweets csv file path")
parser.add_argument("--verbose", action='store_true', help="maximum output")
opts = parser.parse_args()

import numpy as np
from pandas import read_csv

from services.general import MessageService
from services.pipelines import PreProcessingPipelineWrapper
from services.postgres import PostgresReaderService
from utilities.postgres_queries import all_tweets_qry

msg_svc = MessageService(print_level = 2 if opts.verbose else 1)

pipeline = PreProcessingPipelineWrapper(opts.conf_file)

data_svc = PostgresReaderService()

sentences = data_svc.query(all_tweets_qry(['id','text']))

assert False
sentences = read_csv(opts.input_tweets)['text'].values
sentences = list(filter(lambda s: s is not np.nan, sentences))

out = list(pipeline.transform(sentences[:61]))


for i in out[:61]: print(i)
