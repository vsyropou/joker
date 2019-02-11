from utilities.postgres_queries import insert_qry
from utilities.general import info, warn, error, debug

def persist(backend, insert_qry):

    committed = False
    try:
        committed = backend.execute_insert(insert_qry)
        debug('Excecuted query: %s'%insert_qry)

    except Exception as err:

        if err.pgcode == '23505':
            warn('Caught primary key vioaltion, when %s'%insert_qry)
        else:
            error('Throwing unknown runtime exception, when: %s'%insert_qry)
            print(err,err.pgcode)
            raise

    return committed


def persist_sentences(*args):

    try: # parse args
        db   = args[0] # db_backend
        data = args[1] # raw isnert_data
        name = args[2] # table_name
    except KeyError as err:
        error('Not enough arguments to persist sentences')
        raise err

    # helping stuff
    row_to_string = lambda row: "(%s, '{%s}')"%(row.values[0],row.values[1])
    insert_frmter = lambda row: row_to_string(row).replace('[','').replace(']','')

    # prepare insert
    if data.shape[0] == 0:
        responce = []
        warn('Nothing to persist.')
    else:
        insert_data = data.apply(insert_frmter, axis=1)
        responce = [persist(db, insert_qry(name, row)) for row in insert_data]

    return responce
    

def persist_unknown_words(*args):
    # TODO: This needs to be updated to be compatible with pandas, like the above one
    assert False, 'Unknown words persistance is is not ready yet'
    try: # parse args
        db   = args[0] # db_backend
        data = args[1] # raw isnert data
        name = args[2] # table_name
    except KeyError as err:
        error('Not enough arguments to persist unknown words')
        raise RuntimeError(err)

    # helping stuff
    uwrds = lambda snt: [w for w in snt if str==type(w)!=int]

    unknown_words_nested = [ [(uw,ln) for uw in uwrds(snt)] for snt, ln in zip(snts,lang) if uwrds(snt)]

    unknown_words_flatned = [(uw,l) for unwnst in unknown_words_nested for uw, l in unwnst ]

    insert_data = [row for row in [', '. join(["('%s','%s')"%tpl]) for tpl in unknown_words_flatned]]

    return [ persist(db, insert_qry(name, row)) for row in insert_data]

def persist_urls(*args):
    # TODO: This needs to be updated to be compatible with pandas
    try: # parse args
        db    = args[0] # db_backend
        urlsl = args[1] # nested list of urls
        ids   = args[2] # sentence_ids
        name  = args[3] # table_name
    except KeyError as err:
        error('Not enough arguments to persist urls')
        raise RuntimeError(err)
         
    urls_nested  = [ [(id,url) for url in urls] for id, urls in zip(ids, urlsl) ]

    urls_flatned = [ (id,url) for nurl in urls_nested for id, url in nurl]

    insert_data =  [row for row in [', '. join(["('%s','%s')"%tpl]) for tpl in urls_flatned]]
    
    return [ persist(db, insert_qry(name, row)) for row in insert_data]
        

