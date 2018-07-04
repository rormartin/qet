package kafka // import "github.com/icemobilelab/qet/pkg/kafka"

import (
	"context"
	"github.com/icemobilelab/qet/pkg/transform"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	log "github.com/sirupsen/logrus"
	"math"
	"time"
)

type KafkaReceiver struct {
	topic    string
	group    string
	brokers  []string
	shutdown func()
}

func NewKafkaReceiver() *KafkaReceiver {
	q := KafkaReceiver{}
	return &q
}

func (q *KafkaReceiver) Connect(
	msgs chan transform.DataBlock,
	done chan error,
	logger *log.Entry) error {

	return q.ConnectCustomRetry(
		msgs,
		done,
		3,
		func(retry int) int {
			return 10000 * int(math.Pow(2.0, float64(retry)))
		},
		logger)
}

func (q *KafkaReceiver) ConnectCustomRetry(
	msgs chan transform.DataBlock,
	done chan error,
	maxRetries int,
	retryFuncTime func(int) int,
	loggerInput *log.Entry) error {

	logger := loggerInput.WithFields(log.Fields{"context": "ConnectCustomRetry"})

	err := q.startConsumer(msgs, maxRetries, retryFuncTime, logger)
	if err != nil {
		logger.Error("error and shutting down: %v", err)
		defer q.Shutdown(logger)
		return err
	}

	return nil

}

func (q *KafkaReceiver) startConsumer(
	msgs chan transform.DataBlock,
	maxRetries int,
	retryExpirationCalc func(int) int,
	loggerInput *log.Entry) error {

	logger := loggerInput.WithFields(log.Fields{
		"context": "startConsumer",
		"group":   q.group,
		"topic":   q.topic,
		"brokers": q.brokers})

	graph := goka.DefineGroup(
		goka.Group(q.group),
		goka.Input(goka.Stream(q.topic), new(codec.Bytes),
			kafkaMsgProcessor(msgs, maxRetries, retryExpirationCalc, logger)),
		goka.Persist(new(codec.Bytes)))
	opts := []goka.ProcessorOption{}

	logger.Println("Starting goka processor")
	processor, err := goka.NewProcessor(q.brokers, graph, opts...)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	q.shutdown = cancel
	return processor.Run(ctx)
}

func (q *KafkaReceiver) Shutdown(loggerInput *log.Entry) error {
	logger := loggerInput.WithFields(log.Fields{"context": "Shutdown"})

	logger.Println("Shutting down goka processors")
	q.shutdown()
	return nil
}

// Message processor: encapsulate the goka processor with domain injections
func kafkaMsgProcessor(
	output chan transform.DataBlock,
	maxRetries int,
	retryExpirationCalc func(int) int,
	loggerInput *log.Entry) func(ctx goka.Context, msg interface{}) {

	logger := loggerInput.WithFields(log.Fields{"context": "kafkaMsgProcessor"})

	return func(ctx goka.Context, msg interface{}) {
		logger.Println("Message received")

		data := msg.([]byte)

		result := make(chan bool, 1)
		timeout := make(chan bool, 1)

		db := transform.DataBlock{
			Data: data,
			Ack:  func() error { result <- true; return nil },
			Nack: func() error { result <- false; return nil },
		}

		output <- db

		retries := 0
		// blocking waiting for response
		select {
		case res := <-result:
			if res {
				// it's ok, just finish and go for next message
				return
			}
			// !res
			// error, retry mechanism
			if retries >= maxRetries {
				// TODO: review what to do, error topic?
				// not possible to recover from error
				logger.Printf("Too much retries, not possible to process the message")
				return
			}
			// timeout define by the function
			go func() {
				time.Sleep(time.Duration(retryExpirationCalc(retries)) * time.Millisecond)
				timeout <- true
			}()

		case <-timeout:
			// retry to process the message again
			output <- db
			retries++
		}
	}
}
