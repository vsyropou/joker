
import re
import nltk
import numpy as np

from services.pipelines import BasePipelineComponent

class StopWordsRemoverSvc(BasePipelineComponent):
    
    def __init__(self, *args, **kwargs):

        # TODO: # print info when configuring
        lang = args[0] if args[0] else 'english'

        # TODO: make more compact
        # TODO: fix logic to dl only when stopwords cannot be imported
        try:
            self._stop_words = nltk.corpus.stopwords.words(lang)
        except LookupError as err:
            print('Falied to import stopwords. Trying to download')

        try:
            nltk.download('stopwords')
        except Exception as err:
            print('Falied to download english stopwords. Cannot use this component') 

        self._stop_words = nltk.corpus.stopwords.words(lang)

    def transform(self, sents):

        is_not_stopword = lambda w: w not in self._stop_words
        # TODO: # print info when trnasforming
        return filter(is_not_stopword, sents)
    

class TweeterTokenizerSvc(BasePipelineComponent):

    @property
    def wraped_class_def(self):
        return ['nltk', 'TweetTokenizer']
    
    def transform(self, sents):
        return np.array([self.component_instanse.tokenize(s) for s in sents])


class UrlRemover(BasePipelineComponent):

    def transform(self, sents):

        #TODO: Persist the url. setup db to generate hashes on insertion
        if self.persist_removed_urls:
            urls = re.findall("(?P<url>https?://[^\s]+)", ' '.join(sents))
            if urls:
                print('persisting urls to db')

        # remove urls
        out_sentences = [re.sub("(?P<url>https?://[^\s]+)", '', s) for s in sents]
        
        return out_sentences

#TODO:
# use my msg service

