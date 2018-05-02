package transform // import "github.com/icemobilelab/qet/pkg/transform"

import (
	log "github.com/sirupsen/logrus"
	"os"
)

type DataBlock struct {
	Data []byte
	Ack  func() error
	Nack func() error
}

func processX(
	raw []byte,
	configuration map[string]interface{},
	payloadGenerator PayloadGenerator,
	executor func([]byte) error) error {

	payload, err := payloadGenerator.GenEventPayload(raw, configuration)
	if err != nil {
		return err
	}

	return executor(payload)
}

func Processor(
	pg PayloadGenerator,
	configuration map[string]interface{},
	exec func([]byte) error,
	input <-chan DataBlock,
	signals <-chan os.Signal,
	logger *log.Entry) {
	for {
		select {
		case data := <-input:
			err := processX(data.Data, configuration, pg, exec)
			if err != nil {
				logger.Errorf("Error in transaction process: %v", err)
				data.Nack()
			} else {
				data.Ack()
			}
		case s := <-signals:
			log.Printf("Processor stopped via signal: %v", s)
			return
		}
	}
}
