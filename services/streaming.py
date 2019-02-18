
import abc
import time
import numpy as np

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
        
        # TODO: check all parsed args
        cr = bdbcknd.cursor()
        cr.execute(query)

        self.nrows = cr.rowcount
        self.batch_size = step
        
        self.column_names = [d.name for d in cr.description]

        # format records
        self._generator = (formater(cr.fetchmany(step)) for _ in range(0,self.nrows,step))

        self._enter_message = 'Start sql streaming of: %s\n'%query +\
                              ' total number of rows: %s\n'%self.nrows +\
                              ' batch size: %s'%self.batch_size
    def __enter__(self):
        info(self._enter_message)
        return self._generator

    def __exit__(self, *args):
        if not any(args):
            info('Processed stream')

    def __call__(self):
        return self.__enter__()
    
    def dict_formater(self, recs):
        return [{nam:val for nam, val in zip(self.column_names,rec)} for rec in recs]

    def list_formater(self, recs):
        return recs

    def tuple_formater(self, recs):
        return tuple(recs)


class SqlStreamTransformer():

    def __init__(self, *args, nthreads = 1):

        # parse args
        try:
            self._pipeline = args[0]
            self._streamer = args[1]
        except Exception as err:
            error('naaaaaa')
            print(err)

        self._num_threads = nthreads

        self.input_count  = np.int64(0)
        self.output_count = np.int64(0)

        self.time_batch_excecution = False
        self._time_measurements = []

    @property
    def efficiency(self):
        return '%.3f'%(np.float64(float(self.output_count)) / np.float64(self.input_count))

    @property
    def average_execution_time(self):
        return (np.average(self._time_measurements),np.std(self._time_measurements))
        
    def process(self):

        self._pipeline.configure()

        with self._streamer as strm :
            
            num_records = self._streamer.nrows
            batch_size = self._streamer.batch_size

            nam = 'pipline "%s", %s'%(self._pipeline.name,self._pipeline.version)

            with Progress(num_records, name=nam) as prog:

                if self._num_threads == 1: # singlethread

                    results = [self._process_batch(b, prg=(prog,batch_size)) for b in strm]

                else: # multithread
                    pool = ThreadPool(processes=self._num_threads)

                    proxy = lambda b: self._process_batch(b, prg=(prog,batch_size))

                    results = pool.map(proxy, strm)
                    pool.close()
                    pool.join()

        info('pipeline efficiency: %s'%self.efficiency)

        return [r for r in results]

    
    def _process_batch(self, btch, prg=None):

        # timing
        if self.time_batch_excecution:  start = time.time()

        processed_data = [out for out in self._pipeline.transform(btch)]

        # timming
        if self.time_batch_excecution:
            self._time_measurements += [time.time() - start]
            info('Average batch execution time: %.2f +/- %.2f'%(self.average_execution_time))

        # progress report
        if prg: prg[0](jump=prg[1])

        # measure pipeline efficiency 
        self.input_count  += np.int64(self._streamer.batch_size)
        self.output_count += np.int64(len(processed_data))
        
        return processed_data

