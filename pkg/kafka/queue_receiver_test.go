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
