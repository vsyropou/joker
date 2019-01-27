
import re
import numpy as np
from nltk.corpus import stopwords
from string import punctuation

from services.pipelines import BasePipelineComponent
from utilities.general import info, warn, error, debug


class BaseRegExpService(BasePipelineComponent):

    def transform(self, sents):
        info('Transforming sentences')
        return [re.sub(self._regular_expresion, '', snt) for snt in sents]


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

        self._stop_words =  stopwords.words(self.language)
        self._stop_words += list(map(str.lower, self.add_stopwords))

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

    _tokenizer = lambda self, snt: snt.lower().split()

    def transform(self, sents):
        info('Transforming sentences')

        return np.array([self._tokenizer(s) for s in sents])


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
