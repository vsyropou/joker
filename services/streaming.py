
from multiprocessing.pool import ThreadPool

from services.general import MessageService
from utilities.general import info, warn, error, debug
from utilities.general import Progress

# class AbsReadStreamer():
#     # define itenterface

class SqlReadStreamer():
    
    def __init__(self, *args, **kwargs):

        bdbcknd = args[0]
        query = args[1]

        step = kwargs.pop('step', 1)
        rec_frmt = kwargs.pop('records_format', 'dict')
        
        # TODO: check all args
        cursor = bdbcknd.cursor()
        cursor.execute(query)

        self.nrows = cursor.rowcount
        self.batch_size = step
        
        self.column_names = [d.name for d in  cursor.description]

        # format
        formater = getattr(self, '%s_formater'%rec_frmt) 
        records  = lambda : formater(cursor.fetchmany(step))
        
        self._generator = (records() for _ in range(0,self.nrows,step))

    def __enter__(self):
        info('Executing stream')
        return self._generator

    def __exit__(self, *args):
        info('Processed stream')

    def __call__(self):
        return self._generator
    
    def dict_formater(self, recs):
        return [{nam:val for nam, val in zip(self.column_names,rec)} for rec in recs]

    def list_formater(self, recs):
        return recs


class BaseSqlStreamTransformer():

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

                if not self._num_threads == 1:

                    results = [self._process_batch(b, prg=(prog,batch_size)) for b in strm]
                
                else:
                    pool = ThreadPool(processes=self._num_threads)

                    proxy = lambda b: self._process_batch(b, prg=(prog,batch_size))

                    results = pool.map(proxy, strm)

                    pool.close()
                    pool.join()

        print(results)
        return [r for r in results]



class TweetSqlStreamParser(BaseSqlStreamTransformer):

    def _process_batch(self, btch, prg=None):

        data_prx = lambda btch: (r['text'] for r in btch)
        data = data_prx(btch)

        self._update_conf(btch)

        plvl = MessageService._print_level
        MessageService.set_print_level(-1)

        self._pipeline = self._pipeline.reconfigure(self._pipeline.conf)

        MessageService.set_print_level(plvl)

        if prg:
            prg[0](jump=prg[1])

        results = [out for out in self._pipeline.transform(data)]

        
        
        return results


    def _update_conf(self, btch):
        #TODO: Make this update obsolete, this is very tedieous

        cnf = self._pipeline.conf
        
        column_getter = lambda cnam, btch: [r[cnam] for r in btch]
        
        cnf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_sentence_ids'] = column_getter('id',btch)


class LiveTweetSqlParser(BaseSqlStreamTransformer):

    def _process_batch(self):
        pass
