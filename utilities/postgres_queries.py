
# get data
all_tweets_qry = lambda cols, tabl='tweets': "SELECT %s "%(','.join(cols)) + \
                                             "FROM %s"%tabl

# get column names of a table
column_names_qry = lambda t: "SELECT column_name " \
                             "FROM information_schema.columns " \
                             "WHERE table_schema = 'public' "\
                             "AND table_name   = '%s' "%t
# basic isenrt 
insert_qry = lambda nam, vals : "INSERT INTO %s"%(nam) + \
                                " VALUES %s"%vals

# copy csv file to table
copy_csv_to_table_qry = lambda tbnam, clnams, csvfile: "COPY %s (%s) "\
                                                       "FROM '%s' "\
                                                       "DELIMITER ',' "\
                                                       "CSV"%(tbnam,', '.join(clnams),csvfile)

# embedings 
create_embs_table_qry = lambda nam, cnam, ctyp: "CREATE TABLE %s"%(nam) + \
                                                " (id SERIAL PRIMARY KEY, " + \
                                                ", ".join([' '.join([n,t,'NOT NULL ']) for n,t in zip(cnam,ctyp)]) +\
                                                ');'

get_embeding_qry = lambda wrd, wmodel, col='id':"SELECT %s FROM %s WHERE word='%s'"%(col, wmodel, wrd)

get_embeding_batch_qry = lambda wrds, wmodel, col='id':"SELECT %s FROM %s "%(col, wmodel) + \
                                                       "WHERE %s"%( ' or '.join([ "word='%s'"%w for w in wrds ]))

# vocabulary
create_unknown_words_table_qry = lambda nam : "CREATE TABLE %s"%(nam) + \
                                              " (word text NOT NULL, lang varchar(5), PRIMARY KEY (word))"

create_embeding_keys_tweets_table_qry = lambda nam : "CREATE TABLE %s"%(nam) + \
                                                     " (tweet_id bigint NOT NULL, embeding_keys_array integer[], PRIMARY KEY (tweet_id))"

# urls
create_urls_table_qry = lambda nam : "CREATE TABLE %s"%(nam) + \
                        " (tweet_id bigint NOT NULL, url text NOT NULL, PRIMARY KEY (tweet_id))"
