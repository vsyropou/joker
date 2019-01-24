
import re
import numpy as np
from nltk.corpus import stopwords
from string import punctuation

from services.pipelines import BasePipelineComponent
from services.general import info, warn, error, debug

class StopWordsRemoverSvc(BasePipelineComponent):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # check attributes
        if not hasattr(self, 'langueage'):
            warn('StopWordsRemoverSvc: assuming default stopwords language "english"')
            self.language = 'english'

        if not hasattr(self, 'add_stopwords'):
            info('StopWordsRemoverSvc: No additinal stopwords added')
            self.add_stopwords = None

        self._stop_words = stopwords.words(self.language) + self.add_stopwords


    def transform(self, sents):
        info('Transforming sentences')

        condition = lambda snt: [w for w in snt if w not in self._stop_words]
        
        return map(condition, sents)


class EmojiReplacerSvc(BasePipelineComponent):

    wraped_class_def = ['emoji', 'demojize']
    
    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        if not hasattr(self, 'delimeters'):
            self.delimeters = [" <","> "]
    
    def transform(self, sents):
        info('Transforming sentences')

        return np.array([self.underlying_engine(s, delimiters=self.delimeters) for s in sents])

class NumberReplacerSvc(BasePipelineComponent):

    wraped_class_def = ['inflect', 'engine']

    def transform(self, sents):
        info('Transforming sentences')

        replace_func = lambda w: number_to_string(w) if w.isnumeric() else w
        to_numeric_representation = lambda w: int(w) if w.isdecimal() else float(w)

        number_to_string = lambda n: self.underlying_engine.number_to_words(n)
        

        condition = lambda snt: [replace_func(w) for w in snt]

        return map(condition, sents)


class TweeterTokenizerSvc(BasePipelineComponent):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        if not hasattr(self, 'lower_case'):
            self.lower_case = true

        if self.lower_case:
            self._tokenizer = lambda snt: snt.lower().split()
        else:
            self._tokenizer = lambda snt: snt.split()

        
    def transform(self, sents):
        info('Transforming sentences')

        return np.array([self._tokenizer(s) for s in sents])

class BaseRegExpService(BasePipelineComponent):

    def transform(self, sents):
        info('Transforming sentences')
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


# TODO:
# Try to express transoform operations with either numpy or pandas (maybe spark also) operators such that you iterate only once



#TODO: Persist the url. setup db to generate hashes on insertion
# if self.persist_removed_urls:
#     urls = re.findall(self._regular_expresion, ' '.join(sents))
#     if urls:
#         print('persisting urls to db')
        
