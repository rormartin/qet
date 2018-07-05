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

package rabbitmq

import (
	// "fmt"
	//	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewRabbitMQ(t *testing.T) {

	exchange := RabbitMQExchange{
		Name:        "exchange_name",
		Type:        "exchange_type",
		Durable:     true,
		AutoDeleted: false,
		Internal:    false,
		NoWait:      false,
		Arguments:   nil,
	}
	queue := RabbitMQQueueDeclare{
		Name:        "queue",
		Durable:     true,
		AutoDeleted: false,
		Exclusive:   false,
		NoWait:      false,
		Arguments:   nil,
	}

	rabbit := NewRabbitMQReceiver(
		"queue_uri",
		exchange,
		queue,
		"queue_tag")

	assert.Equal(t, rabbit.uri, "queue_uri")
	assert.Equal(t, rabbit.consumerTag, "queue_tag")
	assert.Equal(t, rabbit.exchange, exchange)
	assert.Equal(t, rabbit.queue, queue)
	assert.Equal(t, rabbit.routingKey, "")
	assert.Nil(t, rabbit.consumer, nil)

}

// // fake interface implementation to test ACK calls
// type fakeAcknowledger struct {
// 	error bool
// }

// func (a fakeAcknowledger) Ack(tag uint64, multiple bool) error {
// 	if a.error {
// 		return fmt.Errorf("Test error")
// 	}
// 	return nil
// }

// func (a fakeAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
// 	if a.error {
// 		return fmt.Errorf("Test error")
// 	}
// 	return nil
// }

// func (a fakeAcknowledger) Reject(tag uint64, requeue bool) error {
// 	if a.error {
// 		return fmt.Errorf("Test error")
// 	}
// 	return nil
// }

// func testMsg(body []byte, simError bool) amqp.Delivery {
// 	fack := fakeAcknowledger{error: simError}
// 	return amqp.Delivery{
// 		Acknowledger: fack,
// 		Body:         body,
// 	}
// }

// func testExpirationCalc(retries int) int {
// 	return retries
// }

// func TestHandle(t *testing.T) {

// 	var dl chan amqp.Delivery
// 	var done chan error
// 	var output chan DataBlock

// 	t.Run("No messages", func(t *testing.T) {
// 		dl = make(chan amqp.Delivery, 0)
// 		done = make(chan error)
// 		output = make(chan DataBlock)

// 		go handle(dl, done, output, 1)
// 		close(dl)
// 		<-done
// 	})

// t.Run("Message Ok", func(t *testing.T) {
// 	dl = make(chan amqp.Delivery, 1)
// 	done = make(chan error)
// 	output = make(chan DataBlock)

// 	go handle(dl, done, output)
// 	dl <- fakeMsg([]byte("{}"), false)
// 	close(dl)
// 	block := <-output
// 	assert.NotNil(t, block, "Nil data don't expected")
// 	err := block.Ack()
// 	assert.Nil(t, err, "No error expected")
// })

// t.Run("Message Ok ACK error", func(t *testing.T) {
// 	dl = make(chan amqp.Delivery, 1)
// 	done = make(chan error)
// 	output = make(chan DataBlock)

// 	go handle(dl, done, output)
// 	dl <- fakeMsg([]byte("{}"), true)
// 	close(dl)
// 	block := <-output
// 	assert.NotNil(t, block, "Nil data don't expected")
// 	err := block.Ack()
// 	assert.NotNil(t, err, "Error expected")
// })
// }

// func TestShutdown(t *testing.T) {

// 	ll := &log.Logger{
// 		Out:       os.Stdout,
// 		Formatter: new(log.TextFormatter),
// 		Level:     log.ErrorLevel,
// 	}
// 	logger = ll.WithFields(log.Fields{
// 		"service": "test"})

// 	t.Run("Error no consumer", func(t *testing.T) {
// 		rr := RabbitMQReceiver{
// 			uri:         "",
// 			queue:       "",
// 			consumerTag: "",
// 			logger:      logger,
// 			consumer:    nil,
// 		}

// 		err := rr.Shutdown()
// 		assert.NotNil(t, err, "Error for nil consumer expected")
// 	})

// }
