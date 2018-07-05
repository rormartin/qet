// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
