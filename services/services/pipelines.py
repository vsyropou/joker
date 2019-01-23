import os
import json
import abc
import importlib
import numpy as np

from sklearn.pipeline import Pipeline

__all__ = ['BasePipelineComponent', 'PreProcessingPipelineWrapper']

_import = lambda module_name: importlib.import_module(module_name)   


class PreProcessingPipelineWrapper(Pipeline):

    def __init__(self, conf_file):

        # parse configuration
        cnf = json.load(open(conf_file,'r'))

        steps_specs = cnf.pop('steps', None) 
        memory = cnf.pop('memory', False)

        assert len(steps_specs) >= 1, 'Pipeline without any components.'

        pipeline_steps = []
        for module_name, class_name, step_name in steps_specs:

            # TODO: exception to explain what to do if module is not found
            class_proxy  = getattr(_import(module_name), class_name)

            try:
                class_args   = cnf['%s_conf'%step_name].pop('args', [])
                class_kwargs = cnf['%s_conf'%step_name].pop('kwargs', {})
            except KeyError as err:
                print('Cannot find configuration for underlying engine of class %s. Using defaults.'%class_name)
                class_args, class_kwargs = [], {}

            class_instance = class_proxy(*class_args, **class_kwargs)

            pipeline_steps += [(step_name, class_instance)]

        super().__init__(steps=pipeline_steps, memory=memory)
        # TODO: check that all the steps where included
        # TODO: Print info on steps

class AbsPipelineComponent(abc.ABC):

    @abc.abstractmethod
    def fit(self, sents):
        pass

    @abc.abstractmethod
    def transform(self, sents):
        pass


class BasePipelineComponent(AbsPipelineComponent):

    def __init__(self, *args, **kwargs):
        # TODO: dump configurationon instantiating as a verbose mode
        
        # set attributes, if any, for the wrapper instanse
        for key, arg in kwargs.items():
            if key.startswith('wrapper'):
                setattr(self, '_'.join(key.split('_')[1:]), arg)

        # invoke underlyng object, if any
        if hasattr(self, 'wraped_class_def'):
            module_name = self.wraped_class_def[0]
            class_name  = self.wraped_class_def[1]
            # TODO: Add exception to promt for checking classes excistance, dump suported classes maybe
            try:
                module_proxy = _import(module_name)
                class_proxy  = getattr(module_proxy, class_name)

                # TODO: Print info for instantiating base object
                kwargs = {k:v for k,v in kwargs.items() if not k.startswith('wrapper') }

                # some engines dont need to be initialized
                if class_proxy.__class__.__name__ in ['function', 'LazyModule']:
                    engine = class_proxy
                else:
                    engine = class_proxy(*args, **kwargs)
                # set underlying engine
                setattr(self, "underlying_engine", engine)

            #TODO: parse thjis nicely
            except Exception as err:
                print('Module "%s" is required by "%s" but not found. '
                      'Thowrowing exception.'%(module_name,self.__class__.__name__))
                raise

    def fit(self, sents):
        #TODO: prinout warning that the base method is used and it dows nothing
        return sents
