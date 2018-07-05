package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/icemobilelab/qet/pkg/kafka"
	"github.com/icemobilelab/qet/pkg/transform"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"text/template"
)

var (
	eventsTPL           *template.Template
	logger              *log.Entry
	envServiceName      = os.Getenv("SERVICE_NAME")
	envLogLevelEnv      = os.Getenv("LOG_LEVEL")
	envBrokers          = strings.Split(os.Getenv("QUEUE_BROKERS"), ",")
	envQueueQueue       = os.Getenv("QUEUE_QUEUE")
	envQueueConsumerTag = os.Getenv("QUEUE_CONSUMER_TAG")
)

func init() {

	// init templates folder
	eventsTPL = template.Must(template.ParseGlob("../gotpls/*.gotpl"))

	// init logger
	loglevel, err := log.ParseLevel(strings.ToLower(envLogLevelEnv))
	if err != nil {
		// default level
		loglevel = log.ErrorLevel
	}
	localLogger := &log.Logger{
		Out:       os.Stdout,
		Formatter: new(log.TextFormatter),
		Level:     loglevel,
	}

	logger = localLogger.WithFields(log.Fields{
		"service": envServiceName})
}

func main() {

	configuration := map[string]interface{}{
		"appID":      "appIDContent",
		"key":        "keyContent",
		"apiVersion": "apiVersionContent",
	}

	pg := transform.NewPayloadGenerator(eventsTPL, getTXType, ".gotpl")

	inputStream := make(chan transform.DataBlock)

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// queue data injector
	queue := kafka.NewKafkaReceiver(envBrokers, envQueueConsumerTag, envQueueQueue)

	cpus := runtime.NumCPU()
	logger.Printf("Running using %d parallel process", cpus)
	err := transform.ProcessorOrch(
		queue,
		pg,
		configuration,
		executorSTDOUT,
		inputStream,
		signals,
		logger,
		cpus)

	if err != nil {
		os.Exit(1)
	}
	os.Exit(0)

}

func getTXType(tx map[string]interface{}) ([]string, error) {
	return []string{tx["type"].(string)}, nil
}

func executorSTDOUT(payload []byte) error {

	var output bytes.Buffer
	if err := json.Compact(&output, payload); err != nil {
		return fmt.Errorf("Error in json compacting: %v", err)
	}

	// always block one message
	if strings.Contains(string(output.Bytes()), "35") {
		return fmt.Errorf("Error because it's 35")
	}

	return nil
}
