
import re
import numpy as np
from nltk.corpus import stopwords
from string import punctuation

from services.pipelines import BasePipelineComponent


class StopWordsRemoverSvc(BasePipelineComponent):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # check attributes
        if not hasattr(self, 'langueage'):
            print('StopWordsRemoverSvc: assuming default stopwords language "english"')
            self.language = 'english'

        if not hasattr(self, 'add_stopwords'):
            print('StopWordsRemoverSvc: No additinal stopwords added')
            self.add_stopwords = None

        self._stop_words = stopwords.words(self.language) + self.add_stopwords


    def transform(self, sents):
        print('StopWordsRemoverSvc')

        condition = lambda snt: [w for w in snt if w not in self._stop_words]
        
        return map(condition, sents)


class EmojiReplacerSvc(BasePipelineComponent):

    wraped_class_def = ['emoji', 'demojize']
    
    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        if not hasattr(self, 'delimeters'):
            self.delimeters = [" <","> "]
    
    def transform(self, sents):
        print('EmojiReplacerSvc')
        # TODO: too slow need to vetorize, or find regexp

        return np.array([self.underlying_engine(s, delimiters=self.delimeters) for s in sents])

class NumberReplacerSvc(BasePipelineComponent):

    wraped_class_def = ['inflect', 'engine']

    def transform(self, sents):
        print('NumberReplacerSvc')

        replace_func = lambda w: number_to_string(w) if w.isnumeric() else w
        to_numeric_representation = lambda w: int(w) if w.isdecimal() else float(w)

        number_to_string = lambda n: self.underlying_engine.number_to_words(n)
        

        condition = lambda snt: [replace_func(w) for w in snt]

        return map(condition, sents)


# class NanRemoverSvc(BasePipelineComponent):

#     def transform(self, sents):
#         print('NanRemoverSvc')
#         snt_filter = lambda snt: list(filter(lambda w: w.isalpha(), snt))

#         return map(snt_filter, sents)

class TweeterTokenizerSvc(BasePipelineComponent):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        if not hasattr(self, 'lower_case'):
            self.lower = true
        import pdb; pdb.set_trace()
        if self.lower:
            self._snt_filter = lambda s: s.split().lower()
        else:
            self._snt_filter_filter = lambda s: s.split()

    def transform(self, sents):
        print('TweeterTokenizerSvc')

        return np.array([self._snt_filter(s) for s in sents])

class BaseRegExpService(BasePipelineComponent):

    def transform(self, sents):
        print(self.__class__.__name__)
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
# use my msg service
# print info when trnasforming
# Try to express transoform operations with either numpy or pandas (maybe spark also) operators such that you iterate only once



#TODO: Persist the url. setup db to generate hashes on insertion
# if self.persist_removed_urls:
#     urls = re.findall(self._regular_expresion, ' '.join(sents))
#     if urls:
#         print('persisting urls to db')
        
