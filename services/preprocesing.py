
import re
import abc

import numpy as np
from nltk.corpus import stopwords
from string import punctuation
from inflect import NumOutOfRangeError

from services.pipelines import BasePipelineComponent
from utilities.general import info, warn, error, debug
from utilities.import_tools import instansiate_engine, has_valid_db_backend, has_table
from utilities.persist import persist_sentences, persist_unknown_words, persist_urls
from utilities.postgres_queries import  get_embeding_qry, list_of_tables_qry


class BaseRegExpService(BasePipelineComponent):

    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))        

        sents = super().transform(sents)

        subst = lambda snt: re.sub(self._regular_expresion, '', snt)

        sents[self.operant_column_name] = sents[self.operant_column_name].apply(subst)

        return sents

class RetweetRemoverSvc(BasePipelineComponent):

    _regular_expresion = re.compile('^(RT|rt|RT_|rt_)( @\w*|@\w*|  @\w*)?[: ]')

    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))

        isRetweet = lambda row: not re.match(self._regular_expresion, row[self.operant_column_name])
        
        sents = super().transform(sents)

        return sents[sents.apply(isRetweet, axis=1)]

class LineBreaksRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile("/(\r\n)+|\r+|\n+|\t+/i")

class HandlesRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile("\S*@\S*\s?")

class HashtagRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile("(?:\#+[\w_]+[\w\'_\-]*[\w_]+)")


class PunktuationRemoverSvc(BaseRegExpService):

    _regular_expresion = re.compile('[%s]' % re.escape(punctuation + "…"))

class UrlRemoverSvc(BasePipelineComponent):

    _regular_expresion = re.compile("(?P<url>https?://[^\s]+)")

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # check attributes
        self._check_derived_class_argument(["persist_urls", "sentence_ids", "table_name"],
                                           [False, 'id', "urls"])

        # check that urls can be persisted
        if self.persist_urls:
            try: # data availability
                assert self.sentence_ids
            except AssertionError as err:
                error('"sentence_ids" argument is required when "persist_urls" is True.')
                raise            

            has_valid_db_backend(self)
            has_table(self.db, self.table_name)
                
    def transform(self, sents):

        sents = super().transform(sents)

        tweets    = sents[self.operant_column_name]

        if self.persist_urls:

            urls_list = [re.findall(self._regular_expresion, snt) for snt in tweets.values]
            tweet_ids = sents[self.tweet_ids_column_name].values

            persist_urls(self.db, urls_list, tweet_ids, self.table_name)

        # filter out urls
        sents[self.operant_column_name] =  tweets.apply(lambda snt: re.sub(self._regular_expresion, '',  snt))
        
        return sents


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

        sents = super().transform(sents)
 
        drop_punktuation = lambda snt: [w for w in snt if w not in self._stop_words]
        
        sents[self.operant_column_name] = sents[self.operant_column_name].apply(drop_punktuation)

        return sents


class EmojiReplacerSvc(BasePipelineComponent):

    wraped_class_def = ['emoji', 'demojize']
    
    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self._check_derived_class_argument(['delimeters'], [[" <","> "]])

    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))

        sents = super().transform(sents)

        subst = lambda snt: self.underlying_engine(snt, delimiters=self.delimeters)

        sents[self.operant_column_name] = sents[self.operant_column_name].apply(subst)

        return sents

class NumberReplacerSvc(BasePipelineComponent):

    wraped_class_def = ['inflect', 'engine']

    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))

        sents = super().transform(sents)

        def number_to_string(num):
            try:
                string = self.underlying_engine.number_to_words(num)
            except NumOutOfRangeError:
                warn('NumOutOfRangeError caught from inflect engine for %s'%num)
                string = ''
            except Exception:
                warn('Caught unknown exception from inflect engine')
                string = ''
            return string
        
        replace_func = lambda w: number_to_string(w) if w.isnumeric() else w
        
        filter_numbers = lambda snt: [replace_func(w) for w in snt]

        sents[self.operant_column_name] = sents[self.operant_column_name].apply(filter_numbers)

        return sents


class TweeterTokenizerSvc(BasePipelineComponent):

    _tokenizer = lambda self, snt: snt.lower().split()

    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))

        sents = super().transform(sents)
        
        sents[self.operant_column_name] = sents[self.operant_column_name].apply(self._tokenizer)

        return sents


class WordEmbedingsPgSvc(BasePipelineComponent):

    def __init__(self, *args, **kwargs):
        #TODO: reduce the size of checks ????.....
        super().__init__(*args, **kwargs)

        # check attributes
        self._check_derived_class_argument(["persist_sentences", "persist_unknown_words",
                                            "tweet_ids_column_name", "language_column_name", 'table_names'],
                                           [False, False, "id", "lang", {}])

        # insertion metrics
        self.metrics = {key: None for key in ['persist_sentences', 'persist_unknown_words'] if getattr(self,key)}

        # guarantee db engine
        has_valid_db_backend(self)
        
        # guarantedd language model (word embedings)
        try:
            self.language_model = self.table_names['language_model']
        except KeyError as err:
            error('Specify "wrapper_table_names.language_model" in the pipeline conf file')

        has_table(self.db, self.language_model)

        try:
            assert get_embeding_qry
        except AssertionError as err:
            error('Cannot locate "get_embeding_qry" from module utilities.postgres_queries')
            raise

        # guarante persistance of sentences and unknown words
        for flag_name in ['persist_sentences', 'persist_unknown_words']:
            
            if getattr(self, flag_name):
                try:  # list of tables in the db
                    assert flag_name in self.table_names.keys()
                except KeyError as err:
                    msg = 'Specify wrapper_table_names."%s" in the pipeline conf file'%flag_name
                    error(msg)
                    raise

                has_table(self.db, self.table_names[flag_name])


    def transform(self, sents):
        debug('Progressing %s/%s steps (%s)'%(self.order, self.num_pipeline_steps, self.__class__.__name__))

        sents = super().transform(sents)

        # basic sentence filtering
        sents[self.operant_column_name] = sents[self.operant_column_name].apply(self.sentence_to_embeding_tokens)

        # colect tasks
        operators, arguments = self._collect_tasks(sents)

        results = { nam : opr(*arguments[nam]) for nam, opr in operators.items()}


        # measure persistance fraction
        for key in ['persist_sentences', 'persist_unknown_words']:
            if getattr(self,key):
                self.metrics[key] = {'completed_inserts': float(sum(results[key])) / float(len(results[key]))}

        return results['filter_unknown_words']


    def sentence_to_embeding_tokens(self, snt):
        return [self.word_to_embeding_token(w) for w in snt]

    
    def word_to_embeding_token(self, wrd):

        try:
            response = self.db.execute(get_embeding_qry(wrd, self.language_model))
            assert response
            result = response[0][0]
        except AssertionError:
            debug('Found unknown word "%s"'%wrd)
            result = wrd
        except Exception as err:
            prerror('Caught unknown exception')
            print(err)
            raise

        return result

    def _collect_tasks(self, tokenized_sentences):

        # bookkeeping
        datasets = {'filter_unknown_words': tokenized_sentences[self.operant_column_name]}

        # helping stuff
        is_known   = lambda tok_snt: [w for w in tok_snt if     str!=type(w)==int] 
        is_unknown = lambda tok_snt: [w for w in tok_snt if not str!=type(w)==int]
    
        # standard task    
        operators = {'filter_unknown_words': lambda df: df.apply(is_known).values}
        arguments = {'filter_unknown_words': [datasets['filter_unknown_words']]}

        # append optional tasks
        for tsk_nam, exec_tsk, tsk_func, fltr_fnc, col_nam  in zip(['persist_sentences',     'persist_unknown_words'],
                                                                    [self.persist_sentences,   self.persist_unknown_words],
                                                                    [persist_sentences,        persist_unknown_words],
                                                                    [is_known, is_unknown],
                                                                    [self.operant_column_name, self.language_column_name]
                                                       ):
            if exec_tsk:
                try:
                    data_part  = tokenized_sentences[[self.tweet_ids_column_name, col_nam]]
                    data_slice = data_part[self.operant_column_name].apply(fltr_fnc) 

                    data_part[self.operant_column_name] = data_slice
                    datasets[tsk_nam] = data_part
                except KeyError:
                    error('Cannot locate "%s" required by task "%s"'%(col_nam,tsk_nam))
                    raise
                except Exception as err:
                    error('Caught unknown exception while preparing datasets')
                    print(err)
                    raise

                operators[tsk_nam] = tsk_func
                arguments[tsk_nam] = [self.db, datasets[tsk_nam], self.table_names[tsk_nam]]

        return operators, arguments
