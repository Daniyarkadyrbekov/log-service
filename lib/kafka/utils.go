package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap/zapcore"
)

type TopicPartitionMarshaller kafka.TopicPartition

func (tpm TopicPartitionMarshaller) MarshalLogObject(oe zapcore.ObjectEncoder) error {
	oe.AddString("topic", *tpm.Topic)
	oe.AddInt32("partition", tpm.Partition)
	return nil
}

type TopicPartitionsMarshaller []kafka.TopicPartition

func (tpsm TopicPartitionsMarshaller) MarshalLogArray(ae zapcore.ArrayEncoder) error {
	for _, topic := range tpsm {
		if err := ae.AppendObject(TopicPartitionMarshaller(topic)); err != nil {
			return err
		}
	}
	return nil
}
