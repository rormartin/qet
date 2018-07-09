# Queue Extract and Transformation library

A helper library to help to implement ETL process or process that
requires of consuming data from a queue and execute transformations.

The library uses the standard [template
package](https://golang.org/pkg/text/template/) to define the
transformation between the source Json file and the generation of the
payload (that can be a Json, a SQL sentence or whatever text file
format)

A processor orchestrates the consumption of data from a queue and
launches parallel payload generators to execute the
transformations. The result of the transformations is input for the
user-defined executor.

Multiple source queues can be implemented. The project currently
supports:
* [RabbitMQ](http://www.rabbitmq.com/), via the [streadway/amqp Go
  client](https://golang.org/pkg/text/template/)
* [Kafka](http://kafka.apache.org/), via the [Goka stream processing
  library](https://github.com/lovoo/goka)

Error management is included, support retry mechanism (different
implementation per queue) and reconnect mechanism to deal with service
interruptions in the source queue.

The errors managed by the retry mechanism are:
* error in payload generation (no transformation available or error to
  execute the template)
* error in queue connectivity
* error in payload execution (implemented by the user and error
  propagated to the library), like error calling an API or an error in
  a database insertion

## Working flow:

```
+--------------------------------+
|             Queue              |
|                                |
| json-1, json-2, json-3, json-n |
+---------------+----------------+
                |
               json
                |
     +----------v---------+
     | Payload Generator  |
     |                    |
     |     template 1     |
     |     template 2     |
     |     template n     |
     +----------+---------+
                |
             payload
                |
      +---------v--------+
      |     Executor     |
      +------------------+
```

## Use library dependencies

To obtain the library:

```
go get github.com/icemobilelab/qet
```

Libraries to include:
* base transformation suite and processor:
  `github.com/icemobilelab/qet/pkg/transform`
* RabbitMQ data source: `github.com/icemobilelab/qet/pkg/rabbitmq`
* Kafka data source: `github.com/icemobilelab/qet/pkg/kafka`

## Library dependencies

The dependencies are managed using the standard
[Dep](https://golang.github.io/dep/).
* [Mergo](https://github.com/imdario/mergo) as a helper for the
  transformation library to inject data for the template
  transformations
* [Logrus](https://github.com/sirupsen/logrus) as a structured logger
  system for Go
* [Go RabbitMQ Client Library](https://github.com/streadway/amqp)
* [Goka](https://github.com/lovoo/goka) as a Kafka client
* [Testify](https://github.com/stretchr/testify) as helper for testing

## Project structure

The project contains the main libraries under the `pkg` folder and the
`examples` folder with a implementation reference examples.
