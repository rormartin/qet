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
	brokers  []string
	group    string
	topic    string
	shutdown func()
}

func NewKafkaReceiver(brokers []string, group, topic string) *KafkaReceiver {
	q := KafkaReceiver{
		brokers: brokers,
		group:   group,
		topic:   topic,
	}
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
			return 1000 * int(math.Pow(2.0, float64(retry)))
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

		result := make(chan bool, maxRetries+1)

		db := transform.DataBlock{
			Data: data,
			Ack:  func() error { result <- true; return nil },
			Nack: func() error { result <- false; return nil },
		}

		output <- db

		retries := 0
		// blocking waiting for response
		for {
			select {
			case res := <-result:
				if res {
					// it's ok, just finish and go for next message
					logger.Printf("Success on message process")
					return
				}
				// !res
				// error, retry mechanism
				if retries >= maxRetries {
					// TODO: review what to do, error topic?
					// not possible to recover from error
					logger.Warnf("Too much retries, not possible to process the message")
					return
				}
				// timeout define by the function
				delay := time.Duration(retryExpirationCalc(retries)) * time.Millisecond
				logger.Debugf("Waiting on retry %v for %v", retries, delay)
				timer := time.NewTimer(delay)
				<-timer.C

				// retry to process the message again
				logger.Printf("Retry %v", retries)
				output <- db
				retries++
			}
		}
	}
}
