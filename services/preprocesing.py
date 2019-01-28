
import re
import abc
import numpy as np

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
        for arg, val in zip(["language_model", "persist_sentences", "persist_unknown_words", "sentence_ids"],
                            ["embedingsglove25", False, False, None]):
            self._check_derived_class_argument(arg, val)

        # embedings engine
        if self.persist_sentences or self.persist_unknown_words:
            self._backend = instansiate_engine('services.postgres', 'PostgresWriterService').query
        else:
            self._backend = instansiate_engine('services.postgres', 'PostgresReaderService').query

        self._embedings_engine = lambda wrd: self._backend(get_embeding_qry(wrd, self.language_model))


    def word_to_embeding(self, wrd):
        #TODO: Too may queries. With custom order by you can get the embedings in the same order as the sentece 

        # lookup embeding
        embd = None
        try: 
            embd = self._embedings_engine(wrd)
        except Exception:
            error("Error during embedings lookup")
            raise

        # handle no embeding case
        if not embd:
            warn('Language model "%s" does not include the word "%s", ommiting.'%(self.language_model,wrd))

            if self.persist_unknown_words:
                info('Persisting uknown word "%s"'%wrd)
                #TODO: write create table and insert word query 
                # self._engine_backend(<persist_query>)

        return embd[0][0] if embd else None


    def transform(self, sents):
        info('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
        sentence_embeding = lambda snt: [self.word_to_embeding(w) for w in snt]

        embeded_sentences = map(sentence_embeding, sents)
        
        out_sentences = [[w for w in snt if w] for snt in embeded_sentences]

        if self.persist_sentences:
            # import pdb; pdb.set_trace()
            # persist_sentence = self._backend("persist_query")
             # embeded_sents)
            info('Will persist tweet, asynchronously')

        return out_sentences
        

