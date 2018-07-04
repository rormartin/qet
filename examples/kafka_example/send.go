package main

import (
	"fmt"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	envServiceName      = os.Getenv("SERVICE_NAME")
	envLogLevelEnv      = os.Getenv("LOG_LEVEL")
	envBrokers          = strings.Split(os.Getenv("QUEUE_BROKERS"), ",")
	envQueueQueue       = os.Getenv("QUEUE_QUEUE")
	envQueueConsumerTag = os.Getenv("QUEUE_CONSUMER_TAG")
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	emitter, err := goka.NewEmitter(envBrokers, goka.Stream(envQueueQueue), new(codec.String))
	failOnError(err, "Failed to connect to Kafka")
	defer emitter.Finish()

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

		err = emitter.EmitSync("", body)
		fmt.Print(".")
		failOnError(err, "Failed to publish a message")

		time.Sleep(1 * time.Millisecond)
	}
}
