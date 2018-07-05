package transform

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"text/template"
)

func TestProcessX(t *testing.T) {

	tmpl := template.Must(template.New("test.gotpl").Parse(testTemplate))

	json := []byte(`{ "test2": "val2" }`)
	config := map[string]interface{}{
		"test1": "val1",
	}

	getType := func(map[string]interface{}) ([]string, error) {
		return []string{"test"}, nil
	}

	pg := NewPayloadGenerator(tmpl, getType, ".gotpl")

	t.Run("OK", func(t *testing.T) {
		executed := false
		executor := func([]byte) error {
			executed = true
			return nil
		}

		err := processX(json, config, pg, executor)
		if err != nil {
			t.Errorf("Error found: %v", err)
		}
		assert.Equal(t, true, executed, "Executor not executed")
	})

	t.Run("Error executor", func(t *testing.T) {
		executor := func([]byte) error {
			return fmt.Errorf("test error")
		}

		err := processX(json, config, pg, executor)
		if err == nil {
			t.Errorf("Error expected (and not found)")
		}
	})

	t.Run("Error payload generator", func(t *testing.T) {
		executor := func([]byte) error {
			return nil
		}

		json := []byte(`{ "test2": "val2" `)
		err := processX(json, config, pg, executor)
		if err == nil {
			t.Errorf("Error expected (and not found)")
		}
	})

}

func TestProcessor(t *testing.T) {

	tmpl := template.Must(template.New("test.gotpl").Parse(testTemplate))

	config := map[string]interface{}{
		"test1": "val1",
	}

	getType := func(map[string]interface{}) ([]string, error) {
		return []string{"test"}, nil
	}

	pg := NewPayloadGenerator(tmpl, getType, ".gotpl")

	executed := false
	executor := func([]byte) error {
		executed = true
		return nil
	}

	input := make(chan DataBlock)
	signals := make(chan os.Signal)

	ll := &log.Logger{
		Out:       os.Stdout,
		Formatter: new(log.TextFormatter),
		Level:     log.ErrorLevel,
	}
	logger := ll.WithFields(log.Fields{
		"service": "test"})

	t.Run("signal interruption", func(t *testing.T) {
		go Processor(pg, config, executor, input, signals, logger)
		signals <- os.Interrupt
		assert.Equal(t, false, executed, "Executor executed")
	})

	t.Run("execution Ok", func(t *testing.T) {
		ackTest := false
		nackTest := false
		go Processor(pg, config, executor, input, signals, logger)
		db := &DataBlock{
			Data: []byte(`{ "test2": "val2" }`),
			Ack:  func() error { ackTest = true; return nil },
			Nack: func() error { nackTest = true; return nil },
		}
		input <- *db
		signals <- os.Interrupt
		assert.Equal(t, true, executed, "Executor not executed")
		assert.Equal(t, true, ackTest, "Ack not sent")
		assert.Equal(t, false, nackTest, "Nack sent")
	})

	t.Run("execution no ACK", func(t *testing.T) {
		executor := func([]byte) error {
			return fmt.Errorf("Test error")
		}

		ackTest := false
		nackTest := false
		go Processor(pg, config, executor, input, signals, logger)
		db := &DataBlock{
			Data: []byte(`{ "test2": "val2" }`),
			Ack:  func() error { ackTest = true; return nil },
			Nack: func() error { nackTest = true; return nil },
		}
		input <- *db
		signals <- os.Interrupt
		assert.Equal(t, true, executed, "Executor not executed")
		assert.Equal(t, false, ackTest, "Ack sent")
		assert.Equal(t, true, nackTest, "Nack not sent")
	})

}
