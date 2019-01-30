
import re
import abc

import numpy as np
from nltk.corpus import stopwords
from string import punctuation
from multiprocessing.pool import ThreadPool 
from asyncpg.exceptions import UniqueViolationError

from services.pipelines import BasePipelineComponent
from utilities.general import info, warn, error, debug
from utilities.import_tools import instansiate_engine
from utilities.postgres_queries import insert_qry, get_embeding_qry, get_embeding_batch_qry


class BaseRegExpService(BasePipelineComponent):

    def transform(self, sents):
        info('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
        return [re.sub(self._regular_expresion, '', snt) for snt in sents]

    
class LineBreaksRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile("/(\r\n)+|\r+|\n+|\t+/i")

class HandlesRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile("\S*@\S*\s?")


class UrlRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile("(?P<url>https?://[^\s]+)")


class HashtagRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile("(?:\#+[\w_]+[\w\'_\-]*[\w_]+)")


class PunktuationRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile('[%s]' % re.escape(punctuation + "…"))


class StopWordsRemoverSvc(BasePipelineComponent):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # check attributes
        for arg, val in zip(["language", "add_stopwords"],
                            ["english", None]):
            self._check_derived_class_argument(arg, val)

        # update stopwords
        self._stop_words =  stopwords.words(self.language)
        self._stop_words += list(map(str.lower, self.add_stopwords))

    def transform(self, sents):
        info('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
        condition = lambda snt: [w for w in snt if w not in self._stop_words]

        return map(condition, sents)


class EmojiReplacerSvc(BasePipelineComponent):
    #TODO: Too slow maybe multithread??
    wraped_class_def = ['emoji', 'demojize']
    
    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self._check_derived_class_argument('delimeters', [" <","> "])

    def transform(self, sents):
        info('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
        return np.array([self.underlying_engine(s, delimiters=self.delimeters) for s in sents])

class NumberReplacerSvc(BasePipelineComponent):

    wraped_class_def = ['inflect', 'engine']

    def transform(self, sents):
        info('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
        replace_func = lambda w: number_to_string(w) if w.isnumeric() else w
        to_numeric_representation = lambda w: int(w) if w.isdecimal() else float(w)

        number_to_string = lambda n: self.underlying_engine.number_to_words(n)

        condition = lambda snt: [replace_func(w) for w in snt]

        return map(condition, sents)


class TweeterTokenizerSvc(BasePipelineComponent):

    _tokenizer = lambda self, snt: snt.lower().split()

    def transform(self, sents):
        info('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
        return np.array([self._tokenizer(s) for s in sents])


class WordEmbedingsPgSvc(BasePipelineComponent):

    def __init__(self, *args, **kwargs):
        
        super().__init__(*args, **kwargs)

        # check attributes
        for arg, val in zip(["language_model", "persist_sentences", "persist_unknown_words",
                             "sentence_ids", "sentence_lang", 'multithread', 'workers'],
                            ["embedingsglove25", False, False, None, None, False, 5]):
            self._check_derived_class_argument(arg, val)
        #TODO: check that you have the ids and language in case the corresponding flags are true

        # embedings engine
        if self.persist_sentences or self.persist_unknown_words:
            self._db_backend = instansiate_engine('services.postgres', 'PostgresWriterService').query
        else:
            self._db_backend = instansiate_engine('services.postgres', 'PostgresReaderService').query

        self._embedings_engine = lambda wrd: self._db_backend(get_embeding_qry(wrd, self.language_model))


    def transform(self, sents):
        info('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
 
        sentence_embeding = lambda snt: [self.word_to_embeding(w) for w in snt]
        embeded_sentences = [sentence_embeding(snt) for snt in sents]

        unkown_words_filter = lambda embd_snts: [[w for w in snt if str!=type(w)==int] for snt in embd_snts]

        # collect tasks
        operators = {}
        for name, func, flag in zip(['unknown_words_filter','persist_sentences',     'persist_unknown_words'],
                                    [unkown_words_filter,   self._persist_sentences, self._persist_unknown_words],
                                    [True,                  self.persist_sentences,  self.persist_unknown_words]):
            if flag:
                operators[name] = func

        # multi or single thread ?
        if self.multithread:
            results = self._transform_multithread(operators, embeded_sentences)
        else:
            results = self._transform_singlethread(operators, embeded_sentences)

        return results['unknown_words_filter']

    def word_to_embeding(self, wrd):

        try: # lookup embeding 
            return self._embedings_engine(wrd)[0][0]
        except Exception:
            return wrd

    def _transform_singlethread(self, operators_dict, embeded_sents):

        return { nam : opr(embeded_sents) for nam, opr in operators_dict.items()}

    def _transform_multithread(self, operators_dict, embeded_sents):
        #TODO: make a non async postgres; or return futures from asyncpg to main thread where there is an event loop  
        pool = ThreadPool(processes=self.workers)

        thread = lambda op: pool.apply_async(op, [embeded_sents]).get()
            
        return { nam : thread(opr) for nam, opr in operators_dict.items()}
        #TODO: Does it really run asscynchronouysly if you hit get immediately?? Investigate

    
    def _persist_sentences(self, embeded_sentences):

        sntn = lambda tpl: [w for w in tpl[0] if str!=type(w)==int]
        twid = lambda tpl: tpl[1]
        wrap = lambda itm: itm.replace("[","'{").replace("]","}'")
        frmt = lambda tpl: wrap('(%s, %s)'%(twid(tpl),sntn(tpl)))

        # prepare query and insert
        insert_data = [frmt(tpl) for tpl in zip(embeded_sentences,
                                                self.sentence_ids)]
        
        table_name = 'tweets_embeding_keys_%s'%self.language_model

        return [persist(self._db_backend,
                        insert_qry(table_name, row))  for row in insert_data]

    def _persist_unknown_words(self, embeded_sents):
        embeded_sents[0] = embeded_sents[0] + ['csacdsacsda']
        
        # helping stuff
        uwrds = lambda snt: [w for w in snt if str==type(w)!=int]

        unknown_words_nested = [ [(uw,ln) for uw in uwrds(snt)] for snt, ln in zip(embeded_sents,
                                                                                   self.sentence_lang) if uwrds(snt)]

        unknown_words_flatned = [(uw,l) for unwnst in unknown_words_nested for uw,l in unwnst ]

        import pdb; pdb.set_trace()

        # todo: fix this
        return [ persist(self._db_backend,
                         insert_qry(', '. join(["('%s','%s')"%row])) )for row in unknown_words_flatned]



# helping stuff
def persist(backend, insert_qry):
    rtrn = True
    #TODO: make table name configurable
    try:
        backend(insert_qry)
        debug('Excecuted query: %s'%insert_qry)
    except UniqueViolationError:
        rtrn = False
    except Exception:
        warn('Cannot excecuted query: %s'%insert_qry)
        rtrn = False

    return rtrn

    
