

default: dep test


# Dependencies
dep: vendor/*
	dep ensure

test: dep test_transform test_rabbitmq

test_transform: pkg/transform/*.go
	go test pkg/transform/*

test_rabbitmq: pkg/rabbitmq/*.go
	go test pkg/rabbitmq/*

