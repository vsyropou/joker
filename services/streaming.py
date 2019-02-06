
from utilities.general import info

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


# class BaseSqlStreamTransformer():

#     def __init__(self,*args):
#         pass
    
#     def process_stream(cnf, ppl_prx, stream, num_threads):
#         # TODO: check parsed args
#         multithread = False
#         with stream as strm :

#             num_records = sql_strm.nrows
#             batch_size = sql_strm.batch_size

#             with Progress(num_records, name='Give pipline a name') as prog:

#                 if not multithread:

#                     results = [process_batch(b, cnf, ppl_prx, prg=(prog,batch_size)) for b in strm]
                
#                 else:
#                     pool = ThreadPool(processes=num_threads)

#                     proxy = lambda b: process_batch(b, cnf, ppl_prx, prg=(prog,batch_size))

#                     results = pool.map(proxy, strm)

#                     pool.close()
#                     pool.join()

#         print(results)
#         return [r for r in results]

#     def process_batch(btch, cnf, ppl, prg=None):

#         data_prx = lambda btch: (r['text'] for r in btch)

#         data = data_prx(btch)

#         update_conf(cnf, btch)

#         plvl = MessageService._print_level
#         MessageService.set_print_level(-1)

#         ppl = ppl.reconfigure(cnf)

#         MessageService.set_print_level(plvl)

#         if prg:
#             prg[0](jump=prg[1])

#         results = [out for out in ppl.transform(data)]

#         return results



# class LiveTweetSqlParser(BaseSqlStreamTransformer):



# class TweetSqlParser(BaseSqlStreamTransformer):

#     def update_conf(cnf, btch):
#         #TODO: add exception
    
#         ids  = lambda btch: [r['id'] for r in btch]
#         lang = lambda btch: [r['lang'] for r in btch]

#         cnf['remove_urls_conf']['kwargs']['wrapper_sentence_ids'] = ids(btch)
#         cnf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_sentence_ids'] = ids(btch) 
#         cnf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_sentence_lang'] = lang(btch)

