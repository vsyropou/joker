import os
import json
import abc
import importlib
import numpy as np
import nltk

from sklearn.pipeline import Pipeline

_import = lambda module_name: importlib.import_module(module_name)   


class AbsPipelineComponent(abc.ABC):

    @abc.abstractmethod
    def __init__(self, *args, **conf):
        pass

    @abc.abstractmethod
    def fit(self, sents):
        pass


class BasePipelineComponent(AbsPipelineComponent):

    def __init__(self, *args):

        # parse args
        module_name   = args[0]
        class_name    = args[1]

        comp_args  = args[2].pop('args', None)
        comp_kargs = args[2].pop('kargs', {})
        
        # cannot continue w/o  these, so dont catch exceptions
        module_proxy = _import(module_name)            
        class_proxy  = getattr(module_proxy, class_name)

        self._base_component_instance = class_proxy(*comp_args, **comp_kwrgs)

    @property
    def component_instanse(self):
        return self._base_component_instance


class StopWordsRemover(BasePipelineComponent):

    def __init__(self, *args, **kwargs):

        lang = args[0] if args[0] else 'english'
        
        try:
            self._stop_words = nltk.corpus.stopwords.words(lang)
        except LookupError as err:
            print('Falied to import stopwords. Trying to download')

        try:
            nltk.download('stopwords')
        except Exception as err:
            print('Falied to download englishs stopwords. Cannot use this component') 

        stop_words = nltk.corpus.stopwords.words(lang)
        self._is_stopword = lambda w: w in stop_words
        
        # call base initializer
        super(BasePipelineComponent, self).__init__(*args, **kwargs)
    
    def fit(self, sents):

        return filter(self._is_stopword, sents)
        


class PreProcessingPipelineWrapper():

    is_local_class = lambda : os.path.basename(__file__).split('.')[0]

    def __init__(self, conf_file):

        # parse configuration
        cnf = json.load(open(conf_file,'r'))

        steps_specs = cnf.pop('steps', None) 
        memory = cnf.pop('memory', False)

        assert len(steps_specs) >= 1, 'Pipeline without any components.'
        pipeline_steps = []
        for module_name, class_name, step_name in steps_specs:

            if self.is_local_class:
                class_proxy  = getattr(_import('services.preprocesing'), class_name)
                class_args   = cnf['%s_conf'%step_name].pop('args', None)
                class_kwargs = cnf['%s_conf'%step_name].pop('kwargs', {})

                class_instance = class_proxy(*class_args, **class_kwargs)

            else:
                class_instance = BasePipelineComponent(module_name, class_name, cnf['%conf'%step_name])

            pipeline_steps += [(step_name, class_instance)]

        # import pdb; pdb.set_trace()
        self._pipeline = Pipeline(steps=pipeline_steps, memory=memory)

    @property
    def pipeline():
        return self._pipeline


