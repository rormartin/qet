#!/bin/sh

docker-machine create --driver virtualbox --virtualbox-memory 4096 landoop
eval $(docker-machine env landoop)
docker run -d --rm -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 \
       -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=192.168.99.100 \
       landoop/fast-data-dev:1.0.1

echo "UI ready at http://192.168.99.100:3030/"

