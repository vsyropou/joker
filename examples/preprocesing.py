
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("conf_file", help="provide configuration file")
parser.add_argument("--input-tweets", help="tweets csv file path")
parser.add_argument("--verbose", action='store_true', help="maximum output")
opts = parser.parse_args()

from services.general import MessageService
from services.pipelines import PipelineWrapper
from services.streaming import SqlReadStreamer, TweetSqlStreamParser

from utilities.postgres_queries import all_tweets_qry
from utilities.import_tools import instansiate_engine
from utilities.general import read_json


# configure services
msg_srvc = MessageService(print_level = 2 if opts.verbose else 1)
dbs_srvc = instansiate_engine('services.postgres', 'PostgresWriterService')
# sql_strm = DataStreamerSql(dbs_srvc, all_tweets_qry(['id','text', 'lang']), step=100)
sql_strm = SqlReadStreamer(dbs_srvc, 'SELECT id,text,lang FROM tweets OFFSET 86100', step=100)


# configure pipeline
cnf = read_json(opts.conf_file)
# TODO: Use some sort of schema registry and column getters instead of
#      manually passing data after configuration. Components should be
# able to precessed the data in an agnostic way
# cnf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_db'] = dbs_srvc
# cnf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_sentence_ids'] = 'ids'

ppl_name    = opts.conf_file.split('/')[-1].split('.json')[0]

ppl_version = cnf['pipeline_version']

pipeline = PipelineWrapper(ppl_name, ppl_version, cnf, delay_conf=True)

#TODO:  hash the conf file and append it to the ppl and print it to the stdoutput

stream_processor = TweetSqlStreamParser(pipeline, sql_strm, nthreads = 1)


out = stream_processor.process()

print(out)
