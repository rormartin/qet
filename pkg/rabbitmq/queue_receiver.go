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

package rabbitmq // import "github.com/icemobilelab/qet/pkg/rabbitmq"

import (
	"fmt"
	"github.com/icemobilelab/qet/pkg/transform"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"math"
	"strconv"
)

type RabbitMQExchange struct {
	Name        string
	Type        string
	Durable     bool
	AutoDeleted bool
	Internal    bool
	NoWait      bool
	Arguments   amqp.Table
}

type RabbitMQQueueDeclare struct {
	Name        string
	Durable     bool
	AutoDeleted bool
	Exclusive   bool
	NoWait      bool
	Arguments   amqp.Table
}

type RabbitMQReceiver struct {
	uri         string
	exchange    RabbitMQExchange
	queue       RabbitMQQueueDeclare
	routingKey  string
	consumerTag string
	*consumer
}

type consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

// structure used to create an exchange, a queue and the binding
// between the queue and the exchange (and the death letter when it's
// available)
type exchangeQueueBind struct {
	exchangeName        string
	queueName           string
	queueType           string
	durable             bool
	autoDeleted         bool
	internal            bool
	noWait              bool
	arguments           amqp.Table
	deathLetterExchange string
}

const (
	retrySuffix = ".retry"
	deathSuffix = ".death"
)

func NewRabbitMQReceiver(uri string, exchange RabbitMQExchange, queue RabbitMQQueueDeclare, consumerTag string) *RabbitMQReceiver {

	q := RabbitMQReceiver{
		uri:         uri,
		exchange:    exchange,
		queue:       queue,
		routingKey:  "",
		consumerTag: consumerTag,
		consumer:    nil,
	}

	return &q
}

func (q *RabbitMQReceiver) Connect(
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

func (q *RabbitMQReceiver) ConnectCustomRetry(
	msgs chan transform.DataBlock,
	done chan error,
	maxRetries int,
	retryFuncTime func(int) int,
	loggerInput *log.Entry) error {

	logger := loggerInput.WithFields(log.Fields{"context": "Connect"})

	err := q.startConsumer(msgs, maxRetries, retryFuncTime, logger)
	if err != nil {
		logger.Error("error and shutting down: %v", err)
		defer q.Shutdown(logger)
		return err
	}

	return nil

}

func (q *RabbitMQReceiver) startConsumer(msgs chan transform.DataBlock, maxRetries int, retryExpirationCalc func(int) int, logger *log.Entry) error {

	log := logger.WithFields(log.Fields{"context": "Consumer"})

	q.consumer = &consumer{
		conn:    nil,
		channel: nil,
		tag:     q.consumerTag,
		done:    make(chan error),
	}

	var err error
	log.Printf("dialing %q", q.uri)
	q.consumer.conn, err = amqp.Dial(q.uri)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}

	log.Printf("got Connection, getting Channel")
	q.consumer.channel, err = q.consumer.conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	// Main input queue
	if err = createExchangeQueueBind(
		q.consumer.channel,
		exchangeQueueBind{
			q.exchange.Name,
			q.queue.Name,
			q.exchange.Type,
			q.exchange.Durable,
			q.exchange.AutoDeleted,
			q.exchange.Internal,
			q.exchange.NoWait,
			q.exchange.Arguments,
			q.exchange.Name + deathSuffix,
		}); err != nil {
		return err
	}

	// Retry queue
	retryExchangeQueueBind := exchangeQueueBind{
		q.exchange.Name + retrySuffix,
		q.queue.Name + retrySuffix,
		q.exchange.Type,
		q.exchange.Durable,
		q.exchange.AutoDeleted,
		q.exchange.Internal,
		q.exchange.NoWait,
		q.exchange.Arguments,
		q.exchange.Name,
	}
	if err = createExchangeQueueBind(
		q.consumer.channel,
		retryExchangeQueueBind); err != nil {
		return err
	}

	// Death queue
	if err = createExchangeQueueBind(
		q.consumer.channel,
		exchangeQueueBind{
			q.exchange.Name + deathSuffix,
			q.queue.Name + deathSuffix,
			q.exchange.Type,
			q.exchange.Durable,
			q.exchange.AutoDeleted,
			q.exchange.Internal,
			q.exchange.NoWait,
			q.exchange.Arguments,
			"",
		}); err != nil {
		return err
	}

	err = q.consumer.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("Failed to set QoS: %v", err)
	}

	log.Printf("Starting Consume (consumer tag %s)", q.consumerTag)
	deliveries, err := q.consumer.channel.Consume(
		q.queue.Name,  // name
		q.consumerTag, // consumerTag,
		false,         // noAck
		false,         // exclusive
		false,         // noLocal
		false,         // noWait
		nil,           // arguments
	)
	if err != nil {
		return fmt.Errorf("Queue Consume: %s", err)
	}

	go handle(deliveries,
		q.consumer.done,
		msgs,
		maxRetries,
		q.uri,
		retryExchangeQueueBind,
		retryExpirationCalc,
		log)

	return nil

}

func createExchangeQueueBind(channel *amqp.Channel, specs exchangeQueueBind) error {

	// create exchange
	if err := channel.ExchangeDeclare(
		specs.exchangeName,
		specs.queueType,
		specs.durable,
		specs.autoDeleted,
		specs.internal,
		specs.noWait,
		specs.arguments,
	); err != nil {
		return err
	}

	var args amqp.Table = nil
	// create the queue
	if len(specs.deathLetterExchange) > 0 {
		args = make(amqp.Table)
		args["x-dead-letter-exchange"] = specs.deathLetterExchange
	}

	if _, err := channel.QueueDeclare(
		specs.queueName,
		specs.durable,
		false,
		false,
		false,
		args,
	); err != nil {
		return err
	}

	// bind the queue to the exchange
	err := channel.QueueBind(
		specs.queueName,
		"",
		specs.exchangeName,
		false,
		nil,
	)

	return err
}

func (q *RabbitMQReceiver) Shutdown(loggerInput *log.Entry) error {

	logger := loggerInput.WithFields(log.Fields{"context": "Shutdown"})

	logger.Printf("Shutdown process")
	if q.consumer == nil {
		return fmt.Errorf("The consumer was not initialized")
	}

	// will close() the deliveries channel
	if err := q.consumer.channel.Cancel(q.consumer.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := q.consumer.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}
	defer logger.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-q.consumer.done

}

func handle(
	deliveries <-chan amqp.Delivery,
	done chan error,
	output chan transform.DataBlock,
	maxRetries int,
	queueUri string,
	retrySpecs exchangeQueueBind,
	retryExpirationCalc func(int) int,
	loggerInput *log.Entry) {

	logger := loggerInput.WithFields(log.Fields{"context": "handle"})

	for d := range deliveries {
		logger.Debugf(
			"got %dB delivery: %q",
			len(d.Body),
			d.Body,
		)
		db := transform.DataBlock{
			Data: d.Body,
			Ack:  func() error { return d.Ack(false) },
			Nack: func() error {
				log := logger.WithFields(log.Fields{"context": "Nack"})

				msg := d

				// use the "free" requeue
				if !msg.Redelivered {
					log.Debugf("Re-queue with no re-delivery")
					return d.Nack(false, !msg.Redelivered)
				}

				// evaluate if the message goes to the retry exchange
				// (retry header present and with the right value)
				headers := msg.Headers
				retries := 0
				var err error
				if val, ok := headers["retries"]; ok {
					retries, err = strconv.Atoi(val.(string))
					if err != nil {
						retries = 0
					}
				}
				log.Debugf("Retries for message: %d", retries)
				if retries >= maxRetries {
					// just Nack and no republish
					// eventually will end into the dead-letter exchange
					log.Printf("Too much retries, nack with no requeue (death letter)")
					return d.Nack(false, false)
				}

				log.Debugf("Republishing in retry queue")
				if err := rePublish(
					&msg,
					retries,
					queueUri,
					retrySpecs,
					retryExpirationCalc,
					log); err != nil {
					log.Warnf("Error in republish: %v", err)
					msg.Nack(false, false)
					return nil
				}

				// everything it's Ok
				// ACK to the original one
				msg.Ack(false)
				log.Debugf("Confirmed and ACK")
				return nil
			},
		}
		output <- db
	}
	logger.Printf("handle: deliveries channel closed")
	done <- nil
}

func rePublish(
	msg *amqp.Delivery,
	retries int,
	queueUri string,
	retrySpecs exchangeQueueBind,
	retryExpirationCalc func(int) int,
	logger *log.Entry) error {

	log := logger.WithFields(log.Fields{"context": "rePublish"})

	log.Printf("dialing %q", queueUri)
	conn, err := amqp.Dial(queueUri)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}

	log.Printf("got Connection, getting Channel")
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	// Retry queue
	if err = createExchangeQueueBind(
		channel,
		retrySpecs); err != nil {
		return err
	}

	pubConfirmation := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	if err := channel.Confirm(false); err != nil {
		log.Errorf("confirm.select destination: %v", err)
		return fmt.Errorf("confirm.select destination: %v", err)
	}

	newHeaders := msg.Headers
	if newHeaders == nil {
		newHeaders = make(amqp.Table)
	}
	newHeaders["retries"] = strconv.Itoa(retries + 1)

	newMsg := amqp.Publishing{
		Headers:         newHeaders,
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      strconv.Itoa(retryExpirationCalc(retries)),
		MessageId:       msg.MessageId,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserId,
		AppId:           msg.AppId,
		Body:            msg.Body,
	}

	if err := channel.Publish(
		retrySpecs.exchangeName,
		"",
		false,
		false,
		newMsg); err != nil {
		return err
	}

	// only ack the source delivery when the destination acks the publishing
	if confirmed := <-pubConfirmation; !confirmed.Ack {
		return fmt.Errorf("Error in message republish")
	}
	return nil
}
