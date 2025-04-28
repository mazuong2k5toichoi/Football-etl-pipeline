include env

build:
	docker-compose build

up:
	docker-compose --env-file env up -d

down:
	docker-compose --env-file env down

restart:
	make down && make up

to_psql:
	docker exec -ti de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

psql_create:
	docker exec -ti de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB} -f /tmp/load_data/psql_schema.sql -a

to_mysql:
	docker exec -it de_mysql mysql --local-infile=1 -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}

to_mysql_root:
	docker exec -it de_mysql mysql -u${MYSQL_ROOT_USER} -p"${MYSQL_ROOT_PASSWORD}" -e "CREATE DATABASE IF NOT EXISTS ${MYSQL_DATABASE};"
	docker exec -it de_mysql mysql -u${MYSQL_ROOT_USER} -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}

mysql_load:
	docker exec -it de_mysql mysql --local_infile -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE} -e"source /tmp/load_data/mysql_load.sql"

mysql_create:
	docker exec -it de_mysql mysql --local_infile -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE} -e"source /tmp/load_data/mysql_schema.sql"

dbt_run: 
	cd etl_pipeline/dbt_transform && dbt run --profiles-dir ./
# dbt_docs:
# 	cd etl_pipeline && dbt docs generate --profiles-dir ./ --project-dir football && dbt docs serve --profiles-dir ./ --project-dir football