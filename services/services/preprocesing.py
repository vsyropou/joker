
import re
import nltk
from string import punctuation
import numpy as np

from services.pipelines import BasePipelineComponent


class StopWordsRemoverSvc(BasePipelineComponent):

    def transform(self, sents):

        stop_words = None
        try:
            stop_words = nltk.corpus.stopwords.words(self.language)
        except LookupError as err:
            print('Falied to import stopwords. Trying to download')

            nltk.download('stopwords')
            stop_words = nltk.corpus.stopwords.words(self.language)

        if not stop_words:
            raise RuntimeError('Cannot load stopwords')
        # TODO: parse this exception nicer

        snt_filter = lambda snt: list(filter(lambda s: s not in stop_words, snt))
        # print(snt_filter(sents[3]))
        # import pdb; pdb.set_trace()

        return map(snt_filter, sents)

    

class EmojiReplacerSvc(BasePipelineComponent):

    @property
    def wraped_class_def(self):
        return ['emoji', 'demojize']
    
    def transform(self, sents):
        # TODO: too slow need to vetorize
        # TODO: move delimiters to the configuration file
        import pdb; pdb.set_trace()
        return np.array([self.underlying_engine(s, delimiters=(':',':')) for s in sents])


class TweeterTokenizerSvc(BasePipelineComponent):

    @property
    def wraped_class_def(self):
        return ['nltk', 'TweetTokenizer']
    
    def transform(self, sents):
        return np.array([self.underlying_engine.tokenize(s) for s in sents])


    
class UrlRegExpRemoverSvc(BasePipelineComponent):

    _regular_expresion = re.compile("(?P<url>https?://[^\s]+)")
    
    def transform(self, sents):

        #TODO: Persist the url. setup db to generate hashes on insertion
        if self.persist_removed_urls:
            urls = re.findall(self._regular_expresion, ' '.join(sents))
            if urls:
                print('persisting urls to db')
        # TODO!!!!! compile the regewp only once
        return [re.sub(self._regular_expresion, '', snt) for snt in sents]


class HashtagRegExpRemoverSvc(BasePipelineComponent):

    _regular_expresion = re.compile("(?:\#+[\w_]+[\w\'_\-]*[\w_]+)")
    
    def transform(self, sents):
        return [re.sub(self._regular_expresion, '', snt) for snt in sents]


class PunktuationExpRemoverSvc(BasePipelineComponent):
   
    _regular_expresion = re.compile('[%s]' % re.escape(punctuation))

    def transform(self, sents):

        return [re.sub(self._regular_expresion, '', snt) for snt in sents]


# TODO:
# use my msg service
# print info when trnasforming
# Try to express transoform operations with either numpy or pandas (maybe spark also) operators such that you iterate only once
