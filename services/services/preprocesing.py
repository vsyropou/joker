
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
        return np.array([self.underlying_obj_instanse.tokenize(s) for s in sents])


class RegExpRemover(BasePipelineComponent):

    _regular_expresions = dict(urls  = "(?P<url>https?://[^\s]+)",
                               htags = "(?:\#+[\w_]+[\w\'_\-]*[\w_]+)")

    _remove_entity = lambda slf, entity: getattr(slf, 'remove_%s'%entity)
    
    def transform(self, sents):

        #TODO: Persist the url. setup db to generate hashes on insertion
        if self.persist_removed_urls:
            urls = re.findall("(?P<url>https?://[^\s]+)", ' '.join(sents))
            if urls:
                print('persisting urls to db')

        # helping stuff
        rm = lambda ent: self._remove_entity(ent)
        replace  = lambda snt, rex: re.sub(rex, '', snt)
        rm_urls  = lambda snt: replace(snt, self._regular_expresions['urls'])
        rm_htags = lambda snt: replace(snt, self._regular_expresions['htags'])

        # remove stuff
        if rm('urls') and rm('htags'):
            out_sentences = [ rm_htags(rm_urls(s)) for s in sents]
        elif rm('urls') and not rm('htags'):
            out_sentences = [ rm_urls(s) for s in sents]
        elif rm('htags') and not rm('urls'):
            out_sentences = [ rm_htags(s) for s in sents]
        else:
            print('doing  nothing')
        
        return out_sentences

#TODO:
# use my msg service

