import traceback
import inspect
import requests
import importlib

class MessageService():

    _instansiated = False
        
    _print_level = 3

    _levels = {'ERR ':-1, 'INFO':0, 'WARN':1, 'DEBG':2}

    _print_filters = [ lambda cls, lvl: cls._levels[lvl] <= cls._print_level 
                       ]
    _printt_formaters = {}
        
    def __init__(self,*args,**kwargs):

        if MessageService._instansiated:
            print('Only one instance of class "%s" is allowed.'%MessageService.__name__)
        else:
            MessageService._instansiated = True
        
            MessageService._print_level  = kwargs.pop('print_level',   MessageService._print_level)
            
            extra_filters = kwargs.pop('print_filters', None)
            if extra_filters:
                MessageService._print_filters += [extra_filters]
            

    @classmethod
    def _my_print(cls,msg,lvl,call_frame):
        
        inf = dict( mesg = msg, levl = lvl,
                    modl = call_frame.filename.split('/')[-1],
                    func = call_frame.name,
                    line = call_frame.lineno
                    )
    
        stack = inspect.stack()
        get_frame = lambda nam, lin: filter(lambda frm: frm.function==nam and frm.lineno==lin, stack)
    
        try:
            f_info = list(get_frame(inf['func'],inf['line']))[0]
            class_name = f_info[0].f_locals['self'].__class__.__name__
        except Exception:
            class_name = ''
    
        inf.update( dict(clas=class_name) )

        if cls._print_desicion(lvl):
            print ( cls._format_output(inf) )

    @classmethod
    def _format_output(cls,inf):

        out = '%s'%inf['levl']
        
        if cls._print_level >= cls._levels['DEBG']:
            out += cls._debug_info(inf)

        out += ': %s'%inf['mesg']
        
        return out

    @classmethod
    def _debug_info(cls,inf):
        
        if inf['func'] == '<module>': # from main
            ret = ': from <main> "{modl}", line {line}'.format(**inf)
        else: # from deeper
            if inf['clas']: # has class
                ret = ': {clas}.{func}() from "{modl}", line {line}'.format(**inf)
            else:
                ret = ': {func}() from "{modl}", line {line}'.format(**inf)

        return ret


    @classmethod
    def _print_desicion(cls,lvl):        
        return all([ filt(cls,lvl) for filt in cls._print_filters])

    
    @staticmethod
    def debug(msg):
    
        MessageService._my_print(msg,'DEBG',traceback.extract_stack()[-2])

    @staticmethod
    def info(msg):
    
        MessageService._my_print(msg,'INFO',traceback.extract_stack()[-2])
    
    @staticmethod
    def error(msg):
    
        MessageService._my_print(msg,'ERR ',traceback.extract_stack()[-2])
    
    @staticmethod
    def warn(msg):
    
        MessageService._my_print(msg,'WARN',traceback.extract_stack()[-2])


info = MessageService.info
error = MessageService.error
warn = MessageService.warn
debug = MessageService.debug


class ChunkNorisJoke():
    
    def __enter__(self):
        return self

    def __exit__(self,*args):

        self.joke()

        if self._joke:
            print('\n %s\n'%self._joke)

    def joke(self):

        try:
            rsp = requests.get('https://api.chucknorris.io/jokes/random')
        except Exception:
            rsp = 'Sorry no jokes today....'

        try:
            self._joke = rsp.json()['value']
        except:
            self._joke = None


_import = lambda module_name: importlib.import_module(module_name)   

def _import_module_proxy(module_name):
    try: # import module
        module_proxy = _import(module_name)
    except Exception:
        error('Cannot import module "%s". Make sure there are no typos'
              'and configure your environment properly.'%module_name)
        raise
    return module_proxy

def _import_class_proxy(module_proxy, class_name):
    try:
        class_proxy  = getattr(module_proxy, class_name)
    except Exception:
        error('Cannot import class "%s" from module "%s"'%(class_name, module_name.__name__))
        raise

    return class_proxy

def _instansiate_engine(class_proxy, args, kwargs):
    try:
        class_instance = class_proxy(*args, **kwargs)
        info('Instansiated class "%s"'%class_proxy.__name__)
        if class_args:   debug(' args %s'%args)
        if class_kwargs: debug(' kwargs %s'%kwargs)
    except Exception:
        print('Cannot instansiate class %s'%(class_proxy.__name__))

    return class_instance

