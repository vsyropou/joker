import argparse
parser = argparse.ArgumentParser()
parser.add_argument("filepath",  help="provide input csv like file path")
parser.add_argument("--save",  action='store_true', help="save model as csv")
parser.add_argument("--compress",  action='store_true', help="compress")
opts = parser.parse_args()

import numpy as np
import pandas as pd
from string import punctuation

word = lambda line: line.split(' ')[0]
embd = lambda line: ['%s'%em for em in line.split(' ')[1:]]

# load language model
print("Reading Glove Model")
with open(opts.filepath,'r') as fp:
        model = [[word(line),  *embd(line)] for line in fp]
print ("Done.",len(model)," words loaded!")

# write to csv
if opts.save:
    print('Preparing to write model to csv')

    # read into DataFrame
    num_dimensions = len(model[0]) -1
    column_names = ['word'] + ['embd_%s'%i for i in range(num_dimensions)]

    embedings_matrix = pd.DataFrame(model, columns=column_names)

    # fix annoying endline characters
    embedings_matrix[column_names[-1]] = embedings_matrix[column_names[-1]].apply(lambda v: v.strip())

    # drop punctuation
    drop_indices = embedings_matrix[embedings_matrix['word'].isin([p for p in punctuation])].index
    embedings_matrix = embedings_matrix.drop(drop_indices)

    # write out
    out_filepath = '.'.join(opts.filepath.split('.')[:-1]) + '.csv'
    embedings_matrix.to_csv('%s%s'%(out_filepath,'.gzip' if opts.compress else ''),
                            sep = ',',
                            header = False,
                            index = False,
                            float_format='%+.6f',
                            compression =  'gzip' if opts.compress else '')
