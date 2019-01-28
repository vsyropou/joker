

all_tweets_qry = lambda cols, tabl='tweets': "SELECT %s "%(','.join(cols)) + \
                                             "FROM %s"%tabl


# get column names of a table
column_names_qry = lambda t: "SELECT column_name " \
                             "FROM information_schema.columns " \
                             "WHERE table_schema = 'public' "\
                             "AND table_name   = '%s' "%t

# create embedings table 
create_embs_table_qry = lambda nam, cnam, ctyp: "CREATE TABLE %s"%(nam) + \
                                                " (id SERIAL PRIMARY KEY, " + \
                                                ", ".join([' '.join([n,t,'NOT NULL ']) for n,t in zip(cnam,ctyp)]) +\
                                                ');'
# copy csv file to table
copy_csv_to_table_qry = lambda tbnam, clnams, csvfile: "COPY %s (%s) "\
                                                       "FROM '%s' "\
                                                       "DELIMITER ',' "\
                                                       "CSV"%(tbnam,', '.join(clnams),csvfile)

get_embeding_qry = lambda wrd, wmodel, col='id':"SELECT %s FROM %s WHERE word='%s'"%(col, wmodel, wrd)

get_embeding_batch_qry = lambda wrds, wmodel, col='id':"SELECT %s FROM %s "%(col, wmodel) + \
                                                       "WHERE %s"%( ' or '.join([ "word='%s'"%w for w in wrds ]))
