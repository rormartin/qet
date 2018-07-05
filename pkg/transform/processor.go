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

package transform // import "github.com/icemobilelab/qet/pkg/transform"

import (
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

type DataBlock struct {
	Data []byte
	Ack  func() error
	Nack func() error
}

type Queue interface {
	Connect(stream chan DataBlock, done chan error, logger *log.Entry) error
	ConnectCustomRetry(
		msgs chan DataBlock,
		done chan error,
		maxRetries int,
		retryFuncTime func(int) int,
		loggerInput *log.Entry) error
	Shutdown(logger *log.Entry) error
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

// Stand alone processor consuming data from the input data chan
// and executing the transformation
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
			start := time.Now()
			err := processX(data.Data, configuration, pg, exec)
			elapsed := time.Since(start)
			logger.Debugf("Time to process element: %s", elapsed)
			if err != nil {
				logger.Warningf("Error in transformation: %v", err)
				data.Nack()
			} else {
				data.Ack()
			}
		case s := <-signals:
			logger.Printf("Processor stopped via signal: %v", s)
			return
		}
	}
}

// Full processor orchestration, to start the go routines with the
// amount of parallel processors defined
func ProcessorOrch(
	queue Queue,
	pg PayloadGenerator,
	configuration map[string]interface{},
	exec func([]byte) error,
	input chan DataBlock,
	signals <-chan os.Signal,
	logger *log.Entry,
	concurrents int) error {

	signalsInternal := make(chan os.Signal, 1)
	for i := 0; i < concurrents; i++ {
		loggerPro := logger.WithFields(log.Fields{"processor": i})
		go Processor(pg, configuration, exec, input, signalsInternal, loggerPro)
	}

	done := make(chan error)
	err := queue.Connect(input, done, logger)
	if err != nil {
		logger.Errorf("Queue connection error, stop processor: %v", err)
		return err
	}

	select {
	case err := <-done:
		logger.Error("Error in queue: %v", err)
	case s := <-signals:
		logger.Printf("Gentle close")
		// forward the signal to all the processors
		for i := 0; i < concurrents; i++ {
			signalsInternal <- s
		}
		err = queue.Shutdown(logger)
		if err != nil {
			logger.Errorf("Error in queue shutdown: %v", err)
			return err
		}
		return nil
	}
	return nil
}
