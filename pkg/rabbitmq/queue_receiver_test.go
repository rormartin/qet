package rabbitmq

// import (
// 	"fmt"
// 	"github.com/streadway/amqp"
// 	"github.com/stretchr/testify/assert"
// 	"testing"
// )

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
