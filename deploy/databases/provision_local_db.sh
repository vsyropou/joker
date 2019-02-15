
TWEETS_CMC_DB="tweets_cmc_db"

TABLE_TWT_EMBD_IDX_ARRAY_NAME="tweets_with_embs_idx_glove25"
TABLE_UNKNOWN_WODS_NAME="unknownwords_glove25"
TABLE_URLS_NAME="urls"
VIEW_DATASET_RAW="dataset_raw"

CREATE_TWT_EMBD_IDX_ARRAY_TABLE_QRY="CREATE TABLE "${TABLE_TWT_EMBD_IDX_ARRAY_NAME}" (tweet_id bigint NOT NULL, embeding_keys_array integer[], PRIMARY KEY (tweet_id))"
CREATE_UNKNOWN_WORDS_TABLE_QRY="CREATE TABLE "${TABLE_UNKNOWN_WODS_NAME}" (word text NOT NULL, lang varchar(5), PRIMARY KEY (word))"
CREATE_URLS_TABLE_QRY="CREATE TABLE "${TABLE_URLS_NAME}" (tweet_id bigint NOT NULL, url text NOT NULL, PRIMARY KEY (tweet_id))"
CREATE_DATASET_RAW_VIEW_QRY="CREATE view ${VIEW_DATASET_RAW} as \
		          with heartbeat as (\
			   select *, \
		            dense_rank() over(partition by \"name\" order by insertion_time asc) as time_idx, \
		            insertion_time as time_zero, \
		            lead(insertion_time, 1, null) over(partition by \"name\" order by insertion_time asc) time_one \
		           from cmcfeeds) \
		          select *, \
		          (select array((select id from tweets as tw where tw.insertion_time between hb.time_zero and hb.time_one))) tweet_ids \
		         from heartbeat as hb \
		         where time_one is not null"

echo 'start db container'
docker run -d -p 5432:5432 -v ~/postgres-tweets/:/var/lib/postgresql/data -v ~/data/nlp:/data --name ${TWEETS_CMC_DB} postgres

echo 'waiting 5 seconds ....'
sleep 5

echo 'create TWT_EMBD_IDX_ARRAY_TABLE'
docker exec -it ${TWEETS_CMC_DB} sh -c "psql -U postgres -c \"${CREATE_TWT_EMBD_IDX_ARRAY_TABLE_QRY}\""

echo 'create UNKNOWN_WORDS_TABLE'
docker exec -it ${TWEETS_CMC_DB} sh -c "psql -U postgres -c \"${CREATE_UNKNOWN_WORDS_TABLE_QRY}\""

echo 'create URLS_TABLE'
docker exec -it ${TWEETS_CMC_DB} sh -c "psql -U postgres -c \"${CREATE_URLS_TABLE_QRY}\""


echo 'create DATASET_RAW_VIEW'
docker exec -it ${TWEETS_CMC_DB} sh -c "psql -U postgres -c \"${CREATE_DATASET_RAW_VIEW_QRY}\""


