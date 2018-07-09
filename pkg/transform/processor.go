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
	"context"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

// DataBlock is a generic structure used as an input for the
// user-defined executor.
//
// The raw data is provided and two methods to acknowledge (Ack) that
// the data was correctly processed or to indicate that the data was
// not processed correctly and allows the library to execute the retry
// mechanism on that date to try to process it later.
type DataBlock struct {
	Data []byte
	Ack  func() error
	Nack func() error
}

// Queue is a general interface to connect with a library specified
// queue implementation
type Queue interface {
	// Connect start a connection with a queue, using the provided
	// context, storing the data from the queue in the stream chan
	// DataBlock, returning errors via the error chan and log the
	// process in the provided logger.
	//
	// The connection uses a default retry mechanism implemented,
	// that can depends on the library implementation. For a
	// custom retry parameters, the "ConnectCustomRetry" can be
	// used.
	Connect(context context.Context, stream chan DataBlock, done chan error, logger *log.Entry) error

	// ConnectCustomRetry start a connection with a queue, using
	// the provided context, storing the data from the queue in
	// the stream chan DataBlock, returning errors via the error
	// chan and log the process in the provided logger.
	//
	// The retry mechanism can be tuned via the specification the
	// maximum amount of retries and a function that define the
	// time (in milliseconds) between the retries, with the retry
	// counter as a parameter (starts on 0 -
	// https://www.cs.utexas.edu/users/EWD/ewd08xx/EWD831.html )
	ConnectCustomRetry(
		context context.Context,
		msgs chan DataBlock,
		done chan error,
		maxRetries int,
		retryFuncTime func(int) int,
		loggerInput *log.Entry) error
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

// Processor defines an stand-alone processor that execute the
// produced function "exec" with all the result of the transformed
// data coming via the "input" channel. The payload generator and the
// configuration will be used for the data transformation. The signals
// provides a mechanism to stop the processor execution and the logger
// trace the processor activity.
//
// For every data received in the "input" chan, a transformation is
// made and the result of the transformation is provided as a
// parameter for the "exec" function.
//
// If the transformation fails, the retry mechanism will be used
//
// If the execution of the "exec" function returns an error, the retry
// mechanism will be used.
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

// ProcessorOrch execute the "Processor" function with an extra level
// of orchestration to start more than one transformation and executor
// process in parallel (specified by the "concurrents"
// parameters). The stop mechanism also handle the stop of all the
// individual workers.
//
// See "Processor" for more information.
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

	errorChan := make(chan error)

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	// start queue
	go func() {
		err := queue.Connect(ctx, input, errorChan, logger)
		if err != nil {
			logger.Errorf("Queue connection error, stop processor: %v", err)
			errorChan <- err
			return
		}
	}()

	select {
	case err := <-errorChan:
		logger.Errorf("Error in queue: %v", err)
		return err
	case s := <-signals:
		logger.Printf("Gentle close")
		// forward the signal to all the processors
		for i := 0; i < concurrents; i++ {
			signalsInternal <- s
		}
	}
	return nil
}
