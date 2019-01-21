
import re
import nltk
from string import punctuation
import numpy as np

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

        # check stopwords
        self._stop_words = None
        try:
            self._stop_words = nltk.corpus.stopwords.words(self.language)
        except LookupError as err:
            print('Falied to import stopwords. Trying to download')

            nltk.download('stopwords')
            stop_words = nltk.corpus.stopwords.words(self.language)

        # raise error if no stop words
        if not self._stop_words:
            raise RuntimeError('Cannot load stopwords')
        else: # append additional stopwords
            self._stop_words += self.add_stopwords

            
    def transform(self, sents):
        print('StopWordsRemoverSvc')

        snt_filter = lambda snt: list(filter(lambda w: w not in self._stop_words, snt))

        return map(snt_filter, sents)


class NanRemoverSvc(BasePipelineComponent):

    def transform(self, sents):
        print('NanRemoverSvc')
        snt_filter = lambda snt: list(filter(lambda w: w.isalpha(), snt))

        return map(snt_filter, sents)

class EmojiReplacerSvc(BasePipelineComponent):

    wraped_class_def = ['emoji', 'demojize']
    
    def transform(self, sents):
        print('EmojiReplacerSvc')
        # TODO: too slow need to vetorize, or find regexp
        # TODO: move delimiters to the configuration file
        
        return np.array([self.underlying_engine(s, delimiters=(' EMO', 'OJI ')) for s in sents])


class TweeterTokenizerSvc(BasePipelineComponent):

    wraped_class_def = ['nltk', 'TweetTokenizer']

    def transform(self, sents):
        print('TweeterTokenizerSvc')
        return np.array([self.underlying_engine.tokenize(s) for s in sents])


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
   
    _regular_expresion = re.compile('[%s]' % re.escape(punctuation))


# TODO:
# make master RegExpClass that builds all regexp into one, or not....
# use my msg service
# print info when trnasforming
# Try to express transoform operations with either numpy or pandas (maybe spark also) operators such that you iterate only once



#TODO: Persist the url. setup db to generate hashes on insertion
# if self.persist_removed_urls:
#     urls = re.findall(self._regular_expresion, ' '.join(sents))
#     if urls:
#         print('persisting urls to db')
        
