
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("conf_file", help="provide configuration file")
parser.add_argument("--input-tweets", help="tweets csv file path")
parser.add_argument("--verbose", action='store_true', help="maximum output")
opts = parser.parse_args()


from services.general import MessageService
from services.pipelines import PreProcessingPipelineWrapper
from utilities.postgres_queries import all_tweets_qry
from utilities.import_tools import instansiate_engine
from utilities.general import read_json

limit = 5 # for developing

# get some data
db_backend  = instansiate_engine('services.postgres', 'PostgresWriterService')
query_result = db_backend.query(all_tweets_qry(['id','text', 'lang']))

tweets = list(map(lambda tpl: tpl[1], query_result))[:limit]
twkeys = list(map(lambda tpl: tpl[0], query_result))[:limit]
twlang = list(map(lambda tpl: tpl[2], query_result))[:limit]


# configure pipeline
conf = read_json(opts.conf_file)

conf['remove_urls_conf']['kwargs']['wrapper_db'] = db_backend
conf['remove_urls_conf']['kwargs']['wrapper_sentence_ids'] = twkeys

conf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_db'] = db_backend
conf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_sentence_ids'] = twkeys
conf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_sentence_lang'] = twlang


# initialize services
msg_svc = MessageService(print_level = 2 if opts.verbose else 1)

pipeline = PreProcessingPipelineWrapper(conf, num_operants=len(tweets))

out = list(pipeline.transform(list(tweets)))


# pipeline.reconfigure(conf, num_operants=len(tweets) )


# total = 100000
# fnc = lambda :sleep(2)

# with Progress(total, name='douplesere') as pr:

#     for i in range(100000):
#         #fnc()
#         pr()
