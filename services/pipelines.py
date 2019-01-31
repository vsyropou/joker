
import abc

from pprint import pprint
from sklearn.pipeline import Pipeline

from utilities.general import info, warn, error, debug
from utilities.import_tools import import_module_proxy, import_class_proxy, instansiate_engine

__all__ = ['BasePipelineComponent', 'PreProcessingPipelineWrapper']

class PreProcessingPipelineWrapper(Pipeline):

    def __init__(self, conf):

        # parse configuration
        try:
            setattr(self, 'version', conf.pop('pipeline_version'))
        except Exception:
            error('Attribute "pipeline_version" is mandatory and was not found in the conf file "%s"'%conf_file)
            raise
        
        memory    = conf.pop('memory', False)
        steps_cnf = conf.pop('steps', None) 

        # create pipline steps
        assert len(steps_cnf) >= 1, 'Pipeline without any components.'
        self.pipeline_steps = self._create_steps(steps_cnf, conf)

        # pipeline backend
        super().__init__(steps=self.pipeline_steps, memory=memory)

        try: # pipline backed
            assert len(steps_cnf) == len(self.steps), \
            'Pipeline components where not appended properly.'
        except AssertionError:
            info('The requested pipeline configuration:')
            pprint(steps_cnf)
            info('Was parsed into the pipeline backed as follows:')
            pprint(self.steps)
            raise

    def _create_steps(self, specs, confs):

        self.pipeline_steps = []
        for order, (module_name, class_name, step_name) in enumerate(specs):

            try: # default conf safety
                args   = confs['%s_conf'%step_name].pop('args', [])
                kwargs = confs['%s_conf'%step_name].pop('kwargs', {})
            except KeyError as err:
                warn('No backend configuration found for %s. Using defaults.'%class_name)
                args, kwargs = [], {}

            kwargs['wrapper_order'] = order + 1
            kwargs['wrapper_num_pipeline_steps'] = len(self.pipeline_steps) + 1
            class_instance = instansiate_engine(module_name, class_name, args, kwargs)

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

            class_proxy = import_class_proxy(module_name, class_name)

            # initialize backend engine
            if class_proxy.__class__.__name__ in ['function', 'LazyModule']:
                # some bakends dont need to be initialized
                engine = class_proxy
            else:
                kwargs = {k:v for k,v in kwargs.items() if not k.startswith('wrapper') }
                engine = instansiate_engine(module_name, class_name, args, kwargs)

            setattr(self, "underlying_engine", engine)

    def _check_derived_class_argument(self, arguments, default_values):

        for arg, val in zip(arguments, default_values):
            if not hasattr(self, arg):
                class_name = self.__class__.__name__
                debug('%s: argument "%s" has no value, assuming default "%s"'%(class_name,
                                                                               arguments,
                                                                               default_values))
                setattr(self, arg, val)
            
    def fit(self, sents):
        warn('Default "%s.fit" method does not do anything'%self.__class__.__name__)
        return sents

    def transoform(self, sents):
        warn('Default "%s.transform" method does not do anything'%self.__class__.__name__)
        return sents

