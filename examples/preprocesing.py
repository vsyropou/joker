
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("conf_file", help="provide configuration file")
parser.add_argument("--input-tweets", help="tweets csv file path")
parser.add_argument("--verbose", action='store_true', help="maximum output")
opts = parser.parse_args()

from services.general import MessageService
from services.pipelines import PipelineWrapper
from services.streaming import SqlReadStreamer, SqlStreamTransformer

from utilities.postgres_queries import all_tweets_qry
from utilities.import_tools import instansiate_engine
from utilities.general import read_json


# configure services
msg_srvc = MessageService(print_level = 2 if opts.verbose else 1)
dbs_srvc = instansiate_engine('services.postgres', 'PostgresWriterService')
# sql_strm = DataStreamerSql(dbs_srvc, all_tweets_qry(['id','text', 'lang']), step=100)
sql_strm = SqlReadStreamer(dbs_srvc, 'SELECT id,text,lang FROM tweets LIMIT 100', step=10)


# configure pipeline
cnf = read_json(opts.conf_file)

ppl_name    = opts.conf_file.split('/')[-1].split('.json')[0]
ppl_version = cnf['pipeline_version']

pipeline = PipelineWrapper(ppl_name, ppl_version, cnf,
                           delay_conf=True,
                           db_backend=dbs_srvc)


stream_processor = SqlStreamTransformer(pipeline, sql_strm, nthreads = 2)

out = stream_processor.process()

print()
print(out)
print()
print('pipeline efficiency = %s +/- ....'%stream_processor.efficiency)
