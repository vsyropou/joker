
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("conf_file", help="provide configuration file")
parser.add_argument("--input-tweets", help="tweets csv file path")
parser.add_argument("--verbose", action='store_true', help="maximum output")
opts = parser.parse_args()

from multiprocessing.pool import ThreadPool 

from services.general import MessageService
from services.pipelines import PipelineWrapper
from services.streaming import DataStreamerSql

from utilities.postgres_queries import all_tweets_qry
from utilities.import_tools import instansiate_engine
from utilities.general import Progress, read_json


# configure services
msg_srvc = MessageService(print_level = 2 if opts.verbose else 1)
dbs_srvc = instansiate_engine('services.postgres', 'PostgresWriterService')
sql_strm = DataStreamerSql(dbs_srvc, all_tweets_qry(['id','text', 'lang']), step=100)
# sql_strm = DataStreamerSql(dbs_srvc, 'SELECT id,text,lang FROM tweets LIMIT 40', step=2)


# configure pipeline
conf = read_json(opts.conf_file)

pipeline = PipelineWrapper()


def process_batch(btch, cnf, ppl, prg=None):

    data_prx = lambda btch: (r['text'] for r in btch)

    data = data_prx(btch)

    update_conf(cnf, btch)

    plvl = MessageService._print_level
    MessageService.set_print_level(-1)

    ppl = ppl.reconfigure(cnf)

    MessageService.set_print_level(plvl)

    if prg:
        prg[0](jump=prg[1])

    results = [out for out in ppl.transform(data)]

    return results

    
def update_conf(cnf, btch):
    #TODO: add exception
    
    ids  = lambda btch: [r['id'] for r in btch]
    lang = lambda btch: [r['lang'] for r in btch]

    cnf['remove_urls_conf']['kwargs']['wrapper_sentence_ids'] = ids(btch)
    cnf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_sentence_ids'] = ids(btch) 
    cnf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_sentence_lang'] = lang(btch)

    conf['remove_urls_conf']['kwargs']['wrapper_db'] = instansiate_engine('services.postgres', 'PostgresWriterService')
    conf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_db'] = instansiate_engine('services.postgres', 'PostgresWriterService')

# event loop
def process_stream(cnf, ppl_prx, stream, num_threads):
    # TODO: check parsed args
    multithread = False

    with stream as strm :

        num_records = sql_strm.nrows
        batch_size = sql_strm.batch_size

        with Progress(num_records, name='Give pipline a name') as prog:

            if not multithread:

                results = [process_batch(b, cnf, ppl_prx, prg=(prog,batch_size)) for b in strm]
                
            else:
                pool = ThreadPool(processes=num_threads)

                proxy = lambda b: process_batch(b, cnf, ppl_prx, prg=(prog,batch_size))

                results = pool.map(proxy, strm)

                pool.close()
                pool.join()

    print(results)
    return [r for r in results]
        
#TODO: all these can go to the pipline interface

out = process_stream(conf, pipeline, sql_strm, 2)
print(out)
