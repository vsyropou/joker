
import re
import abc

import numpy as np
from nltk.corpus import stopwords
from string import punctuation
from multiprocessing.pool import ThreadPool 

from services.pipelines import BasePipelineComponent
from utilities.general import info, warn, error, debug
from utilities.import_tools import instansiate_engine
from utilities.postgres_queries import get_embeding_qry, get_embeding_batch_qry


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
            self._backend = instansiate_engine('services.postgres', 'PostgresWriterService').query
        else:
            self._backend = instansiate_engine('services.postgres', 'PostgresReaderService').query

        self._embedings_engine = lambda wrd: self._backend(get_embeding_qry(wrd, self.language_model))


    def word_to_embeding(self, wrd):

        # lookup embeding
        embd = None
        try: 
            embd = self._embedings_engine(wrd)
        except Exception:
            error("Error during embedings lookup")
            raise

        return embd[0][0] if embd else None


    def transform(self, sents):
        info('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
 
        sentence_embeding = lambda snt: [self.word_to_embeding(w) for w in snt]
        embeded_sentences = [sentence_embeding(snt) for snt in sents ]

        filter_unkown_words = lambda embd_snts: [[w for w in snt if w] for snt in embd_snts]

        # collect tasks
        operators = {}
        for name, func, flag in zip(['unknown_words_filter','persist_sentences',     'persist_unknown_words'],
                                    [filter_unkown_words,   self._persist_sentences, self._persist_unknown_words],
                                    [True,                  self.persist_sentences,  self.persist_unknown_words]):
            if flag:
                operators[name] = func

        # multi or single thread ?
        if self.multithread:
            results = self._transform_multithread(operators, embeded_sentences)
        else:
            results = self._transform_singlethread(operators, embeded_sentences)

        return results['unknown_words_filter']

    def _transform_singlethread(self, operators_dict, embeded_sents):
        print('Not available yet')
        assert False
        
        # iterables = [embeded_sentences, self.sentence_ids, self.sentence_lang]
        # out = [(sntn(tpl),twid(tpl),lang(tpl)) for tpl in zip(*iterables)]
        
    def _transform_multithread(self, operators_dict, embeded_sents):

        pool = ThreadPool(processes=self.workers)

        thread = lambda op: pool.apply_async(op, [embeded_sents]).get()
            
        return { nam : thread(opr) for nam, opr in operators_dict.items()}
        #TODO: Does it really run asscynchronouysly if you hit get immediately?? Investigate

    
    def _persist_sentences(self, embeded_sentences):
        info('Will persist tweet, asynchronously')
        import threading
        print(threading.currentThread().getName())
        sntn = lambda tpl: [w for w in tpl[0]]
        twid = lambda tpl: tpl[1]

        raw_insert_data = [(twid(tpl),sntn(tpl)) for tpl in zip(embeded_sentences,
                                                                self.sentence_ids)]
        
        return True # TOD: if success..

    def _persist_unknown_words(self, embeded_sentences):
        info('Will persist unknown words, asynchronously')
        import threading
        print(threading.currentThread().getName())
        sntn = lambda tpl: [w for w in tpl[0]]
        twid = lambda tpl: tpl[1]
        lang = lambda tpl: tpl[2]

        iterables = [embeded_sentences, self.sentence_ids, self.sentence_lang]
        out = [(sntn(tpl),twid(tpl),lang(tpl)) for tpl in zip(*iterables)]

        # handle no embeding case
        # warn('Language model "%s": ommiting the word "%s".'%(self.language_model,wrd))
        
        return True # TOD: if success..
