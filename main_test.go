package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"testing"
	"time"

	libkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/log-service/lib/kafka"
	"github.com/log-service/lib/log_message"
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
		IndexName:     "test-index",
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

	msgCount := 20
	testHashes := make([]string, 0, msgCount)
	for i := 0; i < msgCount; i++ {
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

		testHashes = append(testHashes, testHash)
	}

	time.Sleep(10 * time.Second)

	{
		elCfg := elasticsearch.Config{
			Addresses: []string{
				cfg.EsAddress,
			},
			Transport: &http.Transport{
				ResponseHeaderTimeout: 10 * time.Second,
				IdleConnTimeout:       10 * time.Second,
			},
		}

		client, err := elasticsearch.NewClient(elCfg)
		require.NoError(t, err)

		for _, val := range testHashes {
			var buf bytes.Buffer
			query := map[string]interface{}{
				"query": map[string]interface{}{
					"match": map[string]interface{}{
						"service-name": val,
					},
				},
			}
			err := json.NewEncoder(&buf).Encode(query)
			require.NoError(t, err)

			res, err := client.Search(
				client.Search.WithContext(context.Background()),
				client.Search.WithIndex(cfg.IndexName),
				client.Search.WithBody(&buf),
				client.Search.WithTrackTotalHits(true),
				client.Search.WithPretty(),
			)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)
			defer res.Body.Close()

			var r map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
				log.Fatalf("Error parsing the response body: %s", err)
			}

			require.Equal(
				t,
				val,
				r["hits"].(map[string]interface{})["hits"].([]interface{})[0].(map[string]interface{})["_source"].(map[string]interface{})["message"],
			)
		}
	}
}
