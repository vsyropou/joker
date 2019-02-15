

TWEETS_CMC_DB="tweets_cmc_db"

echo 'droping table cmcfeeds'
docker exec -it ${TWEETS_CMC_DB} sh -c 'psql -U postgres -c "drop table cmcfeeds;"'

echo 'droping table tweets'
docker exec -it ${TWEETS_CMC_DB} sh -c 'psql -U postgres -c "drop table tweets;"'

echo 'copying tweets tables '
docker exec -it ${TWEETS_CMC_DB} sh -c "pg_dump -U postgres -h 192.168.178.24 postgres -t tweets | psql -U postgres -h localhost postgres"

echo 'copying cmcfeeds tables '
docker exec -it ${TWEETS_CMC_DB} sh -c "pg_dump -U postgres -h 192.168.178.24 postgres -t cmcfeeds | psql -U postgres -h localhost postgres"
