import importlib
from utilities.general import error


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

