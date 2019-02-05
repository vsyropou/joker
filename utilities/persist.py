from utilities.postgres_queries import insert_qry
from utilities.general import info, warn, error, debug

def persist(backend, insert_qry):

    backend.execute_insert(insert_qry)

    debug('Excecuted query: %s'%insert_qry)

    return True


def persist_sentences(*args):

    try: # parse args
        db   = args[0] # db_backend
        snts = args[1] # embeded_sentences
        ids  = args[2] # sentence_ids
        name = args[3] # table_name
    except KeyError as err:
        error('Not enough arguments to persist sentences')
        raise RuntimeError(err)

    # helping stuff
    sntn = lambda tpl: [w for w in tpl[0] if str!=type(w)==int]
    twid = lambda tpl: tpl[1]
    wrap = lambda itm: itm.replace("[","'{").replace("]","}'")
    frmt = lambda tpl: wrap('(%s, %s)'%(twid(tpl),sntn(tpl)))

    # prepare query and insert
    insert_data = [frmt(tpl) for tpl in zip(snts,ids)]

    return [persist(db,insert_qry(name, row)) for row in insert_data]


def persist_unknown_words(*args):
    try: # parse args
        db   = args[0] # db_backend
        snts = args[1] # embeded_sentences
        lang = args[2] # sentence_lang
        name = args[3] # table_name
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
        

