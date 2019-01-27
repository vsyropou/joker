
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

msg_svc = MessageService(print_level = 2 if opts.verbose else 1)

ppl = PreProcessingPipelineWrapper(opts.conf_file)

sentences = read_csv(opts.input_tweets)['text'].values
sentences = list(filter(lambda s: s is not np.nan, sentences))

out = list(ppl.transform(sentences[:61]))


for i in out[:61]: print(i)


# TODO: check tweet parsing once more before going on with embedings
