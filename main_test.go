package main

import (
	"encoding/json"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/log-service/lib/log_message"

	libkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/log-service/lib/kafka"
	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestWritingLogs(t *testing.T) {

	l, err := newLogger()
	require.NoError(t, err)

	cfg := &kafka.Config{
		RequestsTopic: "log-service-request-topic",
		EsAddress:     "http://localhost:9200",
		Brokers:       []string{"localhost:9092"},
		GroupID:       "log-service",
		ConfigMap: libkafka.ConfigMap{
			"session-timeout-ms": "6000",
			"fetch-wait-max-ms":  "10ms",
			"fetch-min-bytes":    "1024",
			"auto-offset-reset":  "earliest",
		},
		PollTimeoutMs: 100,
	}

	require.NoError(t, cfg.Check())

	go startService(l, cfg)

	cm, err := cfg.GetConfigMap()
	require.NoError(t, err)

	producer, err := libkafka.NewProducer(&cm)
	require.NoError(t, err)

	testHash := "auto_test" + strconv.Itoa(rand.Int())
	data, err := json.Marshal(log_message.LogMessage{
		ServiceName: testHash,
		Message:     testHash,
		Payload:     testHash,
	})
	require.NoError(t, err)
	err = producer.Produce(
		&libkafka.Message{
			Value:          data,
			TopicPartition: libkafka.TopicPartition{Topic: &cfg.RequestsTopic, Partition: libkafka.PartitionAny},
		},
		nil,
	)
	require.NoError(t, err)

	time.Sleep(10 * time.Second)

}
