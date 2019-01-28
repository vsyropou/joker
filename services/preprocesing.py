
import re
import abc
import asyncio
import numpy as np
import threading
from nltk.corpus import stopwords
from string import punctuation

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
        for arg, val in zip(["language_model", "persist_sentences", "persist_unknown_words", "sentence_ids", "sentence_lang"],
                            ["embedingsglove25", False, False, None, None]):
            self._check_derived_class_argument(arg, val)
        #TODO: check that you have teh ids and language in case the corresponding flags are true

        # embedings engine
        if self.persist_sentences or self.persist_unknown_words:
            self._backend = instansiate_engine('services.postgres', 'PostgresWriterService').query
        else:
            self._backend = instansiate_engine('services.postgres', 'PostgresReaderService').query

        self._embedings_engine = lambda wrd: self._backend(get_embeding_qry(wrd, self.language_model))


    #def word_to_embeding(self, wrd, lang=None):
    def word_to_embeding(self, wrd):
        #TODO: Too may queries. With custom order by you can get the embedings in the same order as the sentece 

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
        #TODO: split transform in two _transform_ functions
 
        sentence_embeding = lambda snt: [self.word_to_embeding(w) for w in snt]
        embeded_sentences = [sentence_embeding(snt) for snt in sents ]

        # continue assyncchronously
        #TODO: If you cannot do it with asyncio use threadpool that you know better
        async def excecute_threads():
            if self.persist_sentences:
                persist_snt = await self._async_persist_sentences(embeded_sentences)    
            if self.persist_unknown_words:
                persist_unk = await self._async_persist_unknown_words(embeded_sentences)
            print(threading.currentThread().getName())
            return persist_snt, persist_unk

        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(excecute_threads())
        # loop.close()

        print(threading.currentThread().getName())
        import pdb;pdb.set_trace()
        return [[w for w in snt if w] for snt in embeded_sentences]

    async def _async_persist_sentences(self, embeded_sentences):
        info('Will persist tweet, asynchronously')
        print(threading.currentThread().getName())
        sntn = lambda tpl: [w for w in tpl[0]]
        twid = lambda tpl: tpl[1]

        raw_insert_data = [(twid(tpl),sntn(tpl)) for tpl in zip(embeded_sentences,
                                                                self.sentence_ids)]
        
        return True # TOD: if success..

    async def _async_persist_unknown_words(self, embeded_sentences):
        info('Will persist unknown words, asynchronously')
        print(threading.currentThread().getName())
        sntn = lambda tpl: [w for w in tpl[0]]
        twid = lambda tpl: tpl[1]
        lang = lambda tpl: tpl[2]

        iterables = [embeded_sentences, self.sentence_ids, self.sentence_lang]
        out = [(sntn(tpl),twid(tpl),lang(tpl)) for tpl in zip(*iterables)]

        # handle no embeding case
        # warn('Language model "%s": ommiting the word "%s".'%(self.language_model,wrd))
        
        return True # TOD: if success..
