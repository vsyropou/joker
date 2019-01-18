
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("conf_file", help="provide configuration file")
opts = parser.parse_args()


from services.preprocesing import PreProcessingPipelineWrapper

ppl = PreProcessingPipelineWrapper(opts.conf_file).pipeline()


