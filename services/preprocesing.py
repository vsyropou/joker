
import re
import abc

import numpy as np
from nltk.corpus import stopwords
from string import punctuation

from services.pipelines import BasePipelineComponent
from utilities.general import info, warn, error, debug
from utilities.import_tools import instansiate_engine, has_valid_db_backend, has_table
from utilities.persist import persist_sentences, persist_unknown_words, persist_urls
from utilities.postgres_queries import  get_embeding_qry, list_of_tables_qry


class BaseRegExpService(BasePipelineComponent):

    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
        return [re.sub(self._regular_expresion, '', snt) for snt in sents]


class LineBreaksRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile("/(\r\n)+|\r+|\n+|\t+/i")

class HandlesRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile("\S*@\S*\s?")


class UrlRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile("(?P<url>https?://[^\s]+)")

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # check attributes
        self._check_derived_class_argument(["persist_urls", "sentence_ids", "table_name"],
                                           [False, [], "urls"])

        # check that urls can be persisted
        if self.persist_urls:
            try: # data availability
                assert self.sentence_ids
                assert len(self.sentence_ids) == self.num_operants
            except AssertionError as err:
                error('"sentence_ids" argument is required when "persist_urls" is True.')
                raise

            has_valid_db_backend(self)
            has_table(self.db, self.table_name)
                
    def transform(self, sents):

        # collect tasks
        operators = {'url_filter': super(UrlRemoverSvc, self).transform}
        arguments = {'url_filter': [sents]}

        # append optional tasks
        if self.persist_urls:
            urls_list = [re.findall(self._regular_expresion, snt) for snt in sents]
            operators['persist_urls'] = persist_urls
            arguments['persist_urls'] = [self.db, urls_list, self.sentence_ids, self.table_name]

        results = { nam : opr(*arguments[nam]) for nam, opr in operators.items()}

        return results['url_filter']

class HashtagRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile("(?:\#+[\w_]+[\w\'_\-]*[\w_]+)")


class PunktuationRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile('[%s]' % re.escape(punctuation + "…"))


class StopWordsRemoverSvc(BasePipelineComponent):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # check attributes
        self._check_derived_class_argument(["language", "add_stopwords"],
                                           ["english", None])            

        # update stopwords
        self._stop_words =  stopwords.words(self.language)
        self._stop_words += list(map(str.lower, self.add_stopwords))

    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
        condition = lambda snt: [w for w in snt if w not in self._stop_words]

        return map(condition, sents)


class EmojiReplacerSvc(BasePipelineComponent):

    wraped_class_def = ['emoji', 'demojize']
    
    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self._check_derived_class_argument(['delimeters'], [[" <","> "]])

    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
        return np.array([self.underlying_engine(s, delimiters=self.delimeters) for s in sents])

class NumberReplacerSvc(BasePipelineComponent):

    wraped_class_def = ['inflect', 'engine']

    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
        replace_func = lambda w: number_to_string(w) if w.isnumeric() else w
        to_numeric_representation = lambda w: int(w) if w.isdecimal() else float(w)

        number_to_string = lambda n: self.underlying_engine.number_to_words(n)

        condition = lambda snt: [replace_func(w) for w in snt]
        return map(condition, sents)


class TweeterTokenizerSvc(BasePipelineComponent):

    _tokenizer = lambda self, snt: snt.lower().split()

    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))
        return np.array([self._tokenizer(s) for s in sents])


class WordEmbedingsPgSvc(BasePipelineComponent):

    def __init__(self, *args, **kwargs):
        #TODO: reduce the size of checks ????.....
        super().__init__(*args, **kwargs)

        # check attributes
        self._check_derived_class_argument(["persist_sentences", "persist_unknown_words",
                                            "sentence_ids", "sentence_lang", 'table_names'],
                                           [False, False, None, None, {}, False])

        # guarantee db engine
        has_valid_db_backend(self)
        
        # guarantedd language model (word embedings)
        try:
            self.language_model = self.table_names['language_model']
        except KeyError as err:
            error('Specify "wrapper_table_names.language_model" in the pipeline conf file')

        has_table(self.db, self.language_model)

        
        # guarante persistance of sentences and unknown words
        #  data availability
        for arg_name, flag_name, cond in zip(['sentence_ids',                'sentence_lang'],
                                             ['persist_sentences',           'persist_unknown_words'],
                                             [self.sentence_ids is not None, self.sentence_lang is not None]):

            if getattr(self,flag_name):
                try: # ids and languages datasets
                    assert cond
                except AssertionError:
                    error('"%s" argument is required when "%s" flag is True.'%(arg_name,flag_name))
                    raise

                try:  # list of tables in the db
                    assert flag_name in self.table_names.keys()
                except KeyError as err:
                    msg = 'Specify wrapper_table_names."%s" in the pipeline conf file'%flag_name
                    error(msg)
                    raise

                has_table(self.db, table_names[flag_name])


    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))

        # basic sentemnce filtering
        tokenized_sentences = [self.sentence_to_embeding_tokens(snt) for snt in sents]

        # colect tasks
        operators, arguments = self._collect_tasks(tokenized_sentences)

        results = { nam : opr(*arguments[nam]) for nam, opr in operators.items()}

        return results['unknown_words_filter']


    def sentence_to_embeding_tokens(self, snt):
        return [self.word_to_embeding_token(w) for w in snt]

    
    def word_to_embeding_token(self, wrd):
        try:
            return self.db.query(get_embeding_qry(wrd, self.language_model))[0][0]
        except NameError:
            error('Cannot locate "get_embeding_qry" from module utilities.postgres_queries')
            raise
        except Exception:
            return wrd

    def _collect_tasks(self, tokenized_sentences):

        unkown_words_filter = lambda embd_snts: [[w for w in snt if str!=type(w)==int] for snt in embd_snts]

        table_names = lambda key: '%s_%s'%(self.table_names[key], self.language_model)

        base_args = [self.db.query, tokenized_sentences]
        arguments = {'unknown_words_filter':  [tokenized_sentences],
                     'persist_sentences':     base_args + [self.sentence_ids,  table_names('persist_sentences')],
                     'persist_unknown_words': base_args + [self.sentence_lang, table_names('persist_unknown_words')]
                     }

        operators = {}
        for name, func, flag in zip(['unknown_words_filter','persist_sentences',     'persist_unknown_words'],
                                    [unkown_words_filter,   persist_sentences,       persist_unknown_words],
                                    [True,                  self.persist_sentences,  self.persist_unknown_words]):

            if flag:
                operators[name] = func

        return operators, arguments
