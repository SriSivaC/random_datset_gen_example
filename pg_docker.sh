#!/bin/bash
docker_volume_test=$(docker volume ls| grep "pgdata")
if [ -z "$docker_volume_test" ]
then
    docker volume create pgdata
else
    echo 'Volume pgdata exists'
fi
docker stop postgresql
docker rm postgresql
docker run -dt \
    -v pgdata:/var/lib/postgresql/data/pgdata \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    -e POSTGRES_USER=pgadmin \
    -e POSTGRES_PASSWORD=d60w554p \
    -e POSTGRES_DB=metrics \
    -p 5432:5432 \
    --name postgresql postgres

