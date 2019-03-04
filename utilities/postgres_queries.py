
# get list of tables
list_of_tables_qry = "SELECT * FROM information_schema.tables;"

# get data
all_tweets_qry = lambda cols, tabl='tweets': "SELECT %s "%(','.join(cols)) + \
                                             "FROM %s"%tabl

# get column names of a table
column_names_qry = lambda t: "SELECT column_name " \
                             "FROM information_schema.columns " \
                             "WHERE table_schema = 'public' "\
                             "AND table_name   = '%s' "%t
# basic insert 
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

# create views
create_raw_dataset_view_qry = lambda name: 'create view %s as'%name +\
                                           ' with heartbeat as(' +\
	                                   '  select *,' +\
	                                   '   dense_rank() over(partition by "name" order by insertion_time asc) as time_idx,' +\
	                                   '   insertion_time as time_zero,' +\
	                                   '   lead(insertion_time, 1, null) over(partition by "name" order by insertion_time asc) time_one' +\
                                           '  from cmcfeeds)' +\
                                           ' select *,' +\
                                           '  (select array((select id from tweets as tw where tw.insertion_time between hb.time_zero and hb.time_one))) tweet_ids' +\
                                           ' from heartbeat as hb' +\
                                           ' where time_one is not null'


# cmc features table
crypto_features_qry = lambda cols, coin, tabl='v_cmc_features': "SELECT %s "%(','.join(cols)) + \
                                                                "FROM %s AS cft "%tabl + \
                                                                "WHERE cft.coin_name='%s' "%coin + \
                                                                "ORDER BY cft.time_idx"

# refresh mv also
refresh_mv_ref_tweet_ids_time_idx_glove25_qry = "REFRESH MATERIALIZED VIEW view mv_ref_tweet_ids_time_idx_glove25"

# tweet features table
tweet_features_qry = lambda cols, tabl='v_tweet_features_glove25': "SELECT %s "%(','.join(cols)) + \
                                                                   "FROM %s AS tft "%tabl + \
                                                                   "LEFT JOIN mv_ref_tweet_ids_time_idx_glove25 AS rf ON tft.tweet_id=rf.tweet_id " + \
                                                                   "ORDER BY rf.time_idx"

tweet_visible_features_qry = lambda cols, tabl='v_tweet_features_glove25': "SELECT %s "%(','.join(cols)) + \
                                                                           "FROM %s AS tft "%tabl + \
                                                                           "LEFT JOIN mv_ref_tweet_ids_time_idx_glove25 AS rf ON tft.tweet_id=rf.tweet_id " + \
                                                                           "WHERE user_followers_count >=5" + \
                                                                           "ORDER BY rf.time_idx"

# all features table
full_features_table_qry = lambda cols, coin, tabl='v_cmc_features': "SELECT %s "%(','.join(cols)) + \
                                                                    "FROM %s AS cft "%tabl + \
                                                                    "LEFT JOIN mv_ref_tweet_ids_time_idx_glove25 AS rf ON cft.time_idx=rf.tweet_id " + \
                                                                    "LEFT JOIN v_tweet_feaures_glove25 as tft on tft.tweet_id=rf.tweet_id " + \
                                                                    "WHERE cft.coin_name='%s'"%coin + \
                                                                    "ORDER BY rf.time_idx"

# get embedings matrix
embeddings_matrix_qry = lambda dim, tabl: "SELECT id, word, ARRAY[%s] "%', '.join(['em.embd_%s'%i for i in range(dim)]) +\
                                          "FROM %s as em" %tabl
