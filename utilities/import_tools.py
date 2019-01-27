import importlib
from utilities.general import info, error, debug


def import_module_proxy(module_name):

    try:
        module_proxy = importlib.import_module(module_name)
    except Exception:
        error('Cannot import module "%s". Make sure there are no typos'
              'and configure your environment properly.'%module_name)
        raise

    return module_proxy

def import_class_proxy(module_name, class_name):

    module_proxy = import_module_proxy(module_name)
    try:
        class_proxy  = getattr(module_proxy, class_name)
    except Exception:
        error('Cannot import class "%s" from module "%s"'%(class_name, module_name.__name__))
        raise

    return class_proxy

def instansiate_engine(module_name, class_name, args, kwargs):

    class_proxy = import_class_proxy(module_name, class_name)
    try:
        class_instance = class_proxy(*args, **kwargs)
    except Exception:
        print('Cannot instansiate class %s'%(class_proxy.__name__))
        raise

    info('Instansiated class "%s"'%class_proxy.__name__)
    if args:   debug(' args %s'%args)
    if kwargs: debug(' kwargs %s'%kwargs)

    return class_instance

