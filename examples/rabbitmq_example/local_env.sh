#!/bin/sh

export SERVICE_NAME="local-test"
export LOG_LEVEL="INFO"
export QUEUE_URI="amqp://guest:guest@localhost:5672/"
export QUEUE_EXCHANGE="test.exchange"
export QUEUE_EXCHANGE_TYPE="topic"
export QUEUE_QUEUE="test.queue"
export QUEUE_CONSUMER_TAG="test-go-tempro"


