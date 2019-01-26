
import json
import abc

from pprint import pprint
from sklearn.pipeline import Pipeline

from utilities.general import info, warn, error, debug
from utilities.import_tools import _import_module_proxy, _import_class_proxy, _instansiate_engine

__all__ = ['BasePipelineComponent', 'PreProcessingPipelineWrapper']

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

            module_proxy = _import_module_proxy(module_name)

            class_proxy = _import_class_proxy(module_proxy,class_name)
            
            try: # default conf safety
                class_args   = confs['%s_conf'%step_name].pop('args', [])
                class_kwargs = confs['%s_conf'%step_name].pop('kwargs', {})
            except KeyError as err:
                warn('No backend configuration found for %s. Using defaults.'%class_name)
                class_args, class_kwargs = [], {}

            class_instance = _instansiate_engine(class_proxy, class_args, class_kwargs)
                
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

            module_proxy = _import_module_proxy(module_name)
            class_proxy = _import_class_proxy(module_proxy,class_name)

            # initialize backend engine
            if class_proxy.__class__.__name__ in ['function', 'LazyModule']:
                engine = class_proxy # some bakends dont need to be initialized
            else:
                kwargs = {k:v for k,v in kwargs.items() if not k.startswith('wrapper') }

                engine = _instansiate_engine(class_proxy, args, kwargs)

            setattr(self, "underlying_engine", engine)

    def fit(self, sents):
        warn('Default "%s.fit" method does not do anything'%self.__class__.__name__)
        return sents

    def transoform(self, sents):
        warn('Default "%s.transform" method does not do anything'%self.__class__.__name__)
        return sents

