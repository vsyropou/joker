
import numpy as np
import nltk
from services.pipelines import BasePipelineComponent

class StopWordsRemoverSvc(BasePipelineComponent):

    @property
    def wraped_class_def(self):
        return ['','']
    
    def __init__(self, *args, **kwargs):

        # TODO: # print info when configuring
        lang = args[0] if args[0] else 'english'

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

    @property
    def wraped_class_def(self):
        return ['','']

    def transform(self, sents):
        #TODO: has the urls and persist them in a sql table
        import pdb; pdb.set_trace()
        urls = re.findall("(?P<url>https?://[^\s]+)", ' '.join(sents))
# use my msg service

