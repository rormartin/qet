
default: test

test: test_transform test_rabbitmq test_kafka

test_transform: pkg/transform/*.go
	go test pkg/transform/*

test_rabbitmq: pkg/rabbitmq/*.go
	go test pkg/rabbitmq/*

test_kafka: pkg/kafka/*.go
	go test pkg/kafka/*	
