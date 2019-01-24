import os
import json
import abc
import importlib
import numpy as np

from pprint import pprint
from sklearn.pipeline import Pipeline
from services.general import info, warn, error, debug

__all__ = ['BasePipelineComponent', 'PreProcessingPipelineWrapper']

_import = lambda module_name: importlib.import_module(module_name)   

class PreProcessingPipelineWrapper(Pipeline):

    def __init__(self, conf_file):

        # parse configuration
        step_confs = json.load(open(conf_file,'r'))

        step_specs = step_confs.pop('steps', None) 
        memory = step_confs.pop('memory', False)

        assert len(step_specs) >= 1, 'Pipeline without any components.'

        self.pipeline_steps = self._create_steps(step_specs, step_confs)

        # pipeline backend
        super().__init__(steps=self.pipeline_steps, memory=memory)

        try: # pipline backed
            assert len(step_specs) == len(self.steps), \
            'Pipeline components where not appended properly.'
        except AssertionError:
            info('The requested pipeline configuration:')
            pprint(steps_specs)
            info('Was parsed by the pipelien backed as follows:')
            pprint(self.steps)
            raise

    def _create_steps(self, specs, confs):

        self.pipeline_steps = []
        for module_name, class_name, step_name in specs:

            try: # import module
                module_proxy = _import(module_name)
            except Exception:
                error('Cannot import module "%s". Make sure there are no typos'
                      'and configure your environment properly.'%module_name)
                raise

            try: # import backend
                class_proxy  = getattr(_import(module_name), class_name)
            except Exception:
                error('Cannot import class "%s" from module "%s"'%(class_name, module_name))
                raise

            try: # default conf safety
                class_args   = confs['%s_conf'%step_name].pop('args', [])
                class_kwargs = confs['%s_conf'%step_name].pop('kwargs', {})
            except KeyError as err:
                warn('No backend configuration found for %s. Using defaults.'%class_name)
                class_args, class_kwargs = [], {}

            try: # instasiate
                class_instance = class_proxy(*class_args, **class_kwargs)
                info('Instansiated class "%s"'%class_name)
                if class_args:   debug(' args %s'%class_args)
                if class_kwargs: debug(' kwargs %s'%class_kwargs)
            except Exception:
                print('Cannot instansiate class %s'%(class_name))
                
            self.pipeline_steps += [(step_name, class_instance)]
        return self.pipeline_steps


class AbsPipelineComponent(abc.ABC):

    @abc.abstractmethod
    def fit(self, sents):
        pass

    @abc.abstractmethod
    def transform(self, sents):
        pass


class BasePipelineComponent(AbsPipelineComponent):

    def __init__(self, *args, **kwargs):
        
        # set attributes, if any, for the wrapper instanse
        for key, arg in kwargs.items():
            if key.startswith('wrapper'):
                setattr(self, '_'.join(key.split('_')[1:]), arg)

        # invoke underlyng object, if any
        if hasattr(self, 'wraped_class_def'):
            module_name = self.wraped_class_def[0]
            class_name  = self.wraped_class_def[1]
            # TODO: Add exception to promt for checking classes excistance, dump suported classes maybe
            try: # import module
                module_proxy = _import(module_name)
            except Exception:
                error('Cannot import module "%s". Make sure there are no typos'
                      'and configure your environment properly.'%module_name)
                raise

            try: # import backend
                class_proxy  = getattr(module_proxy, class_name)
            except Exception:
                error('Cannot import object "%s" from module"%s".'%(class_name,module_name,))
                raise
            
            kwargs = {k:v for k,v in kwargs.items() if not k.startswith('wrapper') }

            try: # initialize backend
                # some bakends dont need to be initialized
                if class_proxy.__class__.__name__ in ['function', 'LazyModule']:
                    engine = class_proxy
                else:
                    engine = class_proxy(*args, **kwargs)

                info('Configured backend "%s" for derived class "%s"'%(class_name,
                                                                       self.__class__.__name__))
                if args:   debug('  args %s'%args)
                if kwargs: debug('  kwargs %s'%kwargs)
                
                setattr(self, "underlying_engine", engine)
            except Exception as err:
                error('Cannot instansiate backend "%s" required by derived class "%s" . '
                      'Thowrowing exception.'%(class_name,self.__class__.__name__))
                raise

    def fit(self, sents):
        warn('Default "%s.fit" method does not do anything'%self.__class__.__name__)
        return sents

    def transoform(self, sents):
        warn('Default "%s.transform" method does not do anything'%self.__class__.__name__)
        return sents

