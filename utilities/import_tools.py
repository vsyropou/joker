import importlib
from pprint import pprint

from utilities.general import info, error, debug
from utilities.postgres_queries import list_of_tables_qry


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

def instansiate_engine(*arguments):

    # check required args
    assert len(arguments) >= 2, error('Parsed arguments "%s" cannot be used to instantiate class')
    module_name = arguments[0]
    class_name  = arguments[1]
    assert type(module_name) == type(class_name) == str, \
        error('Module and class names must be of "str" type. Got "%s" and "%s" instead.'%(type(module_name),type(class_name)))

    # check optional args
    args   = arguments[2] if len(arguments) >= 3 else []
    kwargs = arguments[3] if len(arguments) == 4 else {}
    if args:   assert type(args) == list,   error('Cannot parse "%s" args correctly'%class_name)
    if kwargs: assert type(kwargs) == dict, error('Cannot parse "%s" kwargs correctly'%class_name)

    # instansiate
    class_proxy = import_class_proxy(module_name, class_name)
    try:
        class_instance = class_proxy(*args, **kwargs)
    except Exception:
        print('Cannot instansiate class "%s"'%(class_proxy.__name__))
        raise

    info('Instansiated class "%s"'%class_proxy.__name__)
    if args:   pprint(' args %s'%args)
    if kwargs: pprint(' kwargs %s'%kwargs)

    return class_instance


def has_valid_db_backend(class_instance):
    try:
        assert hasattr(class_instance, 'db') 
    except KeyError as err:
        exmpl = "conf['map_word_to_embeding_indices_conf']['kwargs']['wrapper_db']=<db-backend-isntance>"
        error('Specify a db backend isntance in your main file, e.g.: %s'%exmpl)
        raise
    try:
        assert hasattr(class_instance.db, 'query')
    except AssertionError as err:
        error('Make sure db instance "%s" has a "query" method'%self.db)
        raise

def has_table(backend, table_name):
    try:
        assert table_name in list(map(lambda e: e[2], backend.query(list_of_tables_qry)))
    except AssertionError as err:
        error('Cannot locate table "%s" in the database'%table_name)
        raise

