
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("filepath",  help="provide input csv like file path")
opts = parser.parse_args()

import pandas as pd
import numpy as np


#TODO: use comprehensions
def loadGloveModel(gloveFile):
    with open(gloveFile,'r') as fp:
        print("Loading Glove Model")
        model = {}
        for line in fp:
            splitLine = line.split()
            word = splitLine[0]
            embedding = np.array([float(val) for val in splitLine[1:]])
            model[word] = embedding
        print ("Done.",len(model)," words loaded!")
        return model


data = loadGloveModel(opts.filepath)

# write out
column_names = ['word'] + ['embd_%s'%i for i in range(25)]

# data.to_csv('test.csv',
#             sep=',', 
#             header=False,
#             index=False,
#             float_format='%.6f')
