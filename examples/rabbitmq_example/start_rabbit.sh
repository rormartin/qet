#!/bin/sh

docker run -d --rm --hostname rabbit --name rabbit -p 4369:4369 -p 5671:5671 -p 5672:5672 -p 15672:15672 rabbitmq
docker exec rabbit rabbitmq-plugins enable rabbitmq_management

echo "UI ready in http://guest:guest@localhost:15672/"

