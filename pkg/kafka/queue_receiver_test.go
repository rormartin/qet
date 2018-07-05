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

package kafka

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewKafkaReceiver(t *testing.T) {

	brokers := []string{"broker1", "broker2"}
	group := "group"
	topic := "topic"
	kafka := NewKafkaReceiver(brokers, group, topic)

	assert.Equal(t, brokers, kafka.brokers)
	assert.Equal(t, group, kafka.group)
	assert.Equal(t, topic, kafka.topic)
	assert.Equal(t, topic+".errors", kafka.errorTopic)
	assert.Nil(t, kafka.shutdown)
}

func TestShutdown(t *testing.T) {

	brokers := []string{"broker1", "broker2"}
	group := "group"
	topic := "topic"
	kafka := NewKafkaReceiver(brokers, group, topic)

	shutdownExecuted := false
	kafka.shutdown = func() {
		shutdownExecuted = true
	}

	localLogger := &log.Logger{}

	kafka.Shutdown(log.NewEntry(localLogger))
	assert.True(t, shutdownExecuted)
}
