package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

var (
	envServiceName       = os.Getenv("SERVICE_NAME")
	envLogLevelEnv       = os.Getenv("LOG_LEVEL")
	envQueueUri          = os.Getenv("QUEUE_URI")
	envQueueQueue        = os.Getenv("QUEUE_QUEUE")
	envQueueExchange     = os.Getenv("QUEUE_EXCHANGE")
	envQueueExchangeType = os.Getenv("QUEUE_EXCHANGE_TYPE")
	envQueueConsumerTag  = os.Getenv("QUEUE_CONSUMER_TAG")
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial(envQueueUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		envQueueExchange,     // name
		envQueueExchangeType, // type
		true,                 // durable
		false,                // auto-deleted
		false,                // internal
		false,                // no-wait
		nil,                  // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	seed := rand.NewSource(time.Now().UnixNano())
	rg := rand.New(seed)

	for {
		testType := func() string {
			i := rg.Intn(2) // [0,2)
			if i == 0 {
				return "collect"
			}
			return "add"
		}()
		body := fmt.Sprintf(`{"type": "%s", "tx" : {"mutation" : %s, "timestamp": "%s" }}`,
			testType,
			strconv.Itoa(rg.Intn(50)),
			time.Now().Format(time.RFC850))

		err = ch.Publish(
			envQueueExchange, // exchange
			"",               // routing key
			false,            // mandatory
			false,            // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(body),
			})
		fmt.Print(".")
		failOnError(err, "Failed to publish a message")

		time.Sleep(1 * time.Millisecond)
	}
}
