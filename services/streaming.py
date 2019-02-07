
import abc
from multiprocessing.pool import ThreadPool

from services.general import MessageService
from utilities.general import info, warn, error, debug
from utilities.general import Progress

class AbsDataStreamer(abc.ABC):
    def __enter__(self):
        pass
    def __init__(self, *args, **kwargs):
        pass
    def __exit__(self, *args):
        pass
    def __call__(self):
        pass

class SqlReadStreamer(AbsDataStreamer):
    
    def __init__(self, *args, **kwargs):

        bdbcknd = args[0]
        query = args[1]

        step = kwargs.pop('step', 1)

        formater = getattr(self, '%s_formater'%kwargs.pop('records_format', 'dict'))
        
        # TODO: check all args
        cr = bdbcknd.cursor()
        cr.execute(query)

        self.nrows = cr.rowcount
        self.batch_size = step
        
        self.column_names = [d.name for d in cr.description]

        # format records
        self._generator = (formater(cr.fetchmany(step)) for _ in range(0,self.nrows,step))

        #TDOO: TOTALLY  replace this with a permanenet solution for configuring pipelines
        self._backed = bdbcknd

    def __enter__(self):
        info('Executing stream')
        return self._generator

    def __exit__(self, *args):
        info('Processed stream')
        if args:
            info(args)

    def __call__(self):
        return self._generator
    
    def dict_formater(self, recs):
        return [{nam:val for nam, val in zip(self.column_names,rec)} for rec in recs]

    def list_formater(self, recs):
        return recs


class StreamTransofrmer(abc.ABC):
    def process(self):
        pass

class BaseSqlStreamTransformer(StreamTransofrmer):

    def __init__(self, *args, nthreads = 1):

        # parse args
        try:
            self._pipeline = args[0]
            self._streamer = args[1]
        except Exception as err:
            error('naaaaaa')
            print(err)

        self._num_threads = nthreads

        self._processed = False

    def process(self):

        with self._streamer as strm :
            
            num_records = self._streamer.nrows
            batch_size = self._streamer.batch_size

            nam = '%s_%s'%(self._pipeline.name,self._pipeline.version)

            with Progress(num_records, name=nam) as prog:

                if self._num_threads == 1:

                    results = [self._process_batch(b, prg=(prog,batch_size)) for b in strm]
                
                else:
                    pool = ThreadPool(processes=self._num_threads)

                    proxy = lambda b: self._process_batch(b, prg=(prog,batch_size))

                    results = pool.map(proxy, strm)
                    #TODO: maybe join before calling map??, I am not sure
                    pool.close()
                    pool.join()

        print(results)
        return [r for r in results]


    def _reconfigure_pipeline(self):

        plvl = MessageService._print_level

        MessageService.set_print_level(-1)

        self._pipeline = self._pipeline.reconfigure(self._pipeline.conf)

        MessageService.set_print_level(plvl)

        
class TweetSqlStreamParser(BaseSqlStreamTransformer):

    def _process_batch(self, btch, prg=None):

        self._update_conf(btch)

        self._reconfigure_pipeline()
        
        if prg: prg[0](jump=prg[1])
        #TODO: this is too rigid, need to make classes
        #     agnostic to the specifics of the dataset
        data = (r['text'] for r in btch)

        return [out for out in self._pipeline.transform(data)]


    def _update_conf(self, btch):
        #TODO: Make this methos obsolete, this is very tedieous, can be done much less coupled

        cnf = self._pipeline.conf

        column_getter = lambda cnam, btch: [r[cnam] for r in btch]
        cnf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_db'] = self._streamer._backed
        cnf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_sentence_ids'] = column_getter('id',btch)


class LiveTweetSqlParser(BaseSqlStreamTransformer):

    def _process_batch(self):
        pass
