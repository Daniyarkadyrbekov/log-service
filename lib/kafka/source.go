package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/log-service/lib/log_message"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func Run(ctx context.Context, log *zap.Logger, config *Config) error {
	cm, err := config.GetConfigMap()
	if err != nil {
		return errors.Wrap(err, "kafka")
	}

	consumer, err := kafka.NewConsumer(&cm)
	if err != nil {
		return errors.Wrap(err, "kafka consumer")
	}

	err = consumer.SubscribeTopics([]string{config.RequestsTopic}, nil)
	if err != nil {
		return errors.Wrap(err, "kafka consumer")
	}

	cfg := elasticsearch.Config{
		Addresses: []string{
			config.EsAddress,
		},
		Transport: &http.Transport{
			ResponseHeaderTimeout: 10 * time.Second,
			IdleConnTimeout:       10 * time.Second,
		},
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Error("error creating es client", zap.Error(err))
		return err
	}

	if _, err := client.Info(); err != nil {
		return err
	}

	go consumerLoop(ctx, log.Named("kafka consumer"), consumer, config.PollTimeoutMs, client)

	return nil
}

func consumerLoop(ctx context.Context, log *zap.Logger, consumer *kafka.Consumer, pollTimeoutMs int, client *elasticsearch.Client) {

	defer consumer.Close()

	done := ctx.Done()

	log.Info("starting consumption")
	for {
		select {
		case <-done:
			return
		default:
			evt := consumer.Poll(0)
			if evt == nil {
				continue
			}
			switch msg := evt.(type) {
			case kafka.AssignedPartitions:
				log.Info("assigned partitions", zap.Array("partitions", TopicPartitionsMarshaller(msg.Partitions)))
				if err := consumer.Assign(msg.Partitions); err != nil {
					log.Fatal("cannot assign partition", zap.Error(err))
				}
			case kafka.RevokedPartitions:
				log.Info("revoked partitions", zap.Array("partitions", TopicPartitionsMarshaller(msg.Partitions)))
				if err := consumer.Unassign(); err != nil {
					log.Fatal("cannot revoke partition", zap.Error(err))
				}
			case *kafka.Message:
				//if err := push.Unmarshal(msg.Value); err != nil {
				//	log.Error("failed to unmarshall push task from kafka", zap.ByteString("value", msg.Value), zap.Error(err))
				//	continue
				//}
				//resp := processor.Send(push)
				//if notEmpty(resp) {
				//	invalidations <- resp
				//}
				//if _, err := consumer.CommitMessage(msg); err != nil {
				//	log.Fatal("cannot commit", zap.Error(err))
				//}

				log.Debug("Consumed message")

				lm := &log_message.LogMessage{}
				if err := json.Unmarshal(msg.Value, lm); err != nil {
					log.Error("error unmarshal msg", zap.Error(err))
					continue
				}
				lmBytes, err := json.Marshal(lm)
				if err != nil {
					continue
				}

				req := esapi.IndexRequest{
					Index:      "test_index2",
					DocumentID: strconv.Itoa(rand.Int()),
					Body:       bytes.NewReader(lmBytes),
					Refresh:    "true",
				}

				res, err := req.Do(context.Background(), client)
				if err != nil {
					log.Error("do req err", zap.Error(err))
				}
				log.Debug("req result", zap.Any("res", res))
				res.Body.Close()

				if _, err := consumer.CommitMessage(msg); err != nil {
					log.Fatal("cannot commit", zap.Error(err))
				}
			case kafka.PartitionEOF:
				log.Info("reached partition end")
			case kafka.Error:
				log.Error("kafka error", zap.Error(msg))
			}
		}
	}
}
