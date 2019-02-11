
def sql_tweets_stream_parser(conf, num_threads=1, time_excecution=False, verbose=False):

    from services.general import MessageService
    from services.pipelines import PipelineWrapper
    from services.streaming import SqlReadStreamer, SqlStreamTransformer

    from utilities.postgres_queries import all_tweets_qry
    from utilities.import_tools import instansiate_engine

    # configure services
    msg_srvc = MessageService(print_level = 2 if verbose else 1)
    dbs_srvc = instansiate_engine('services.postgres', 'PostgresWriterService')
    #sql_strm = SqlReadStreamer(dbs_srvc, all_tweets_qry(['id','text']), step=300)
    sql_strm = SqlReadStreamer(dbs_srvc, 'SELECT id, text FROM tweets LIMIT 20', step=5)


    # configure pipeline
    ppl_name    = opts.conf_file.split('/')[-1].split('.json')[0]
    ppl_version = cnf['pipeline_version']

    pipeline = PipelineWrapper(ppl_name, ppl_version, cnf,
                               delay_conf=True,
                               db_backend=dbs_srvc)

    stream_processor = SqlStreamTransformer(pipeline, sql_strm, nthreads = num_threads)
    stream_processor.time_batch_excecution = time_excecution

    # fire processing
    processed_data = stream_processor.process()

    print()
    print('pipeline efficiency = %s +/- ....'%stream_processor.efficiency)
    if stream_processor.time_batch_excecution:
        print('average execution time (per batch) = %.2f +/- %.2f'%(stream_processor.average_execution_time))
    
    return processed_data, stream_processor


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("conf_file", help="provide configuration file")
    parser.add_argument("--nthreads", default=1, type=int, help="number of trhreads")
    parser.add_argument("--timing", action='store_true', help="time batch eexecution")
    parser.add_argument("--verbose", action='store_true', help="maximum output")
    opts = parser.parse_args()

    from utilities.general import read_json
    cnf = read_json(opts.conf_file)

    # fire stream processing
    output_data, stream_processesor = sql_tweets_stream_parser(cnf,
                                                               num_threads=opts.nthreads,
                                                               time_excecution=opts.timing,
                                                               verbose=opts.verbose)
    
