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
	brokers    []string
	group      string
	topic      string
	errorTopic string
}

func NewKafkaReceiver(brokers []string, group, topic string) *KafkaReceiver {
	q := KafkaReceiver{
		brokers:    brokers,
		group:      group,
		topic:      topic,
		errorTopic: topic + ".errors",
	}
	return &q
}

func (q *KafkaReceiver) Connect(
	ctx context.Context,
	msgs chan transform.DataBlock,
	done chan error,
	logger *log.Entry) error {

	return q.ConnectCustomRetry(
		ctx,
		msgs,
		done,
		3,
		func(retry int) int {
			return 1000 * int(math.Pow(3.0, float64(retry)))
		},
		logger)
}

func (q *KafkaReceiver) ConnectCustomRetry(
	ctx context.Context,
	msgs chan transform.DataBlock,
	done chan error,
	maxRetries int,
	retryFuncTime func(int) int,
	loggerInput *log.Entry) error {

	logger := loggerInput.WithFields(log.Fields{"context": "ConnectCustomRetry"})

	err := q.startConsumer(ctx, msgs, maxRetries, retryFuncTime, logger)
	if err != nil {
		logger.Errorf("Error and shutting down: %v", err)
		return err
	}

	return nil

}

func (q *KafkaReceiver) startConsumer(
	ctx context.Context,
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
			kafkaMsgProcessor(msgs, q.brokers, q.errorTopic, maxRetries, retryExpirationCalc, logger)),
		goka.Persist(new(codec.Bytes)))
	opts := []goka.ProcessorOption{}
	opts = append(opts, goka.WithLogger(logger))

	processor, err := goka.NewProcessor(q.brokers, graph, opts...)
	if err != nil {
		return err
	}

	go runAutoReconnect(ctx, processor, logger) // cancel context will stop the process
	return nil
}

// Manage automatic reconnected mechanism (for ever)
func runAutoReconnect(context context.Context, processor *goka.Processor, loggerInput *log.Entry) {

	logger := loggerInput.WithFields(log.Fields{
		"context": "runAutoReconnect"})

	for {
		// verify valid context
		if context.Err() == nil {
			logger.Println("Starting goka processor")
			err := processor.Run(context)

			if err != nil {
				logger.Errorf("Error in processor start: %v", err)
			}

			logger.Printf("Unexpected ending for processor, trying to run again")

			time.Sleep(5 * time.Second) // arbitrary time to reconnect
		}
	}

}

// Message processor: encapsulate the goka processor with domain injections
func kafkaMsgProcessor(
	output chan transform.DataBlock,
	brokers []string,
	errorTopic string,
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

					// not possible to recover from error: move message to dead-letter and log it
					logger.Warnf("Too much retries, not possible to process the message (copy at %v)", errorTopic)

					pub, err := goka.NewEmitter(brokers, goka.Stream(errorTopic), new(codec.Bytes))
					if err != nil {
						logger.Errorf("Error creating publisher to track error message: %v", err)
						return
					}
					defer pub.Finish()
					err = pub.EmitSync(ctx.Key(), data)
					if err != nil {
						logger.Errorf("Error publishing error message (potentially lost): %v", err)
						return
					}
					logger.Infof("Error message reported correctly to %v", errorTopic)
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
