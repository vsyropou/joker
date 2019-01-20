
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("conf_file", help="provide configuration file")
parser.add_argument("--input-tweets", help="tweets csv file path")
opts = parser.parse_args()

import numpy as np
from pandas import read_csv
from services.pipelines import PreProcessingPipelineWrapper

ppl = PreProcessingPipelineWrapper(opts.conf_file)

sentences = read_csv(opts.input_tweets)['text'].values
sentences = list(filter(lambda s: s is not np.nan, sentences))

out = list(ppl.transform(sentences))

# use pandas dataframes by default, so that you can exploit dask later
