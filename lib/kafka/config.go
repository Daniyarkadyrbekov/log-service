package kafka

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

type Config struct {
	RequestsTopic string          `mapstructure:"requests-topic"`
	IndexName     string          `mapstructure:"index-name"`
	EsAddress     string          `mapstructure:"es-address"`
	EsUsername    string          `mapstructure:"es-username"`
	EsPassword    string          `mapstructure:"es-password"`
	Brokers       []string        `mapstructure:"brokers"`
	GroupID       string          `mapstructure:"group-id"`
	ConfigMap     kafka.ConfigMap `mapstructure:"config"`
	PollTimeoutMs int             `mapstructure:"poll-timeout-ms"`
}

func (c *Config) GetConfigMap() (kafka.ConfigMap, error) {
	cm := cloneConfigMap(c.ConfigMap)

	for k, v := range map[string]string{
		"auto.offset.reset": "earliest",
		"bootstrap.servers": strings.Join(c.Brokers, ","),
		"group.id":          c.GroupID,
	} {
		err := cm.SetKey(k, v)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to set value of %q to %q", k, v)
		}
	}

	return cm, nil
}

func (c *Config) Check() error {
	var err *multierror.Error
	if len(c.Brokers) == 0 {
		err = multierror.Append(err, errors.New("`brokers` list should not be empty"))
	}
	for idx, broker := range c.Brokers {
		if len(broker) == 0 {
			err = multierror.Append(err, fmt.Errorf("`broker.%d` should not be empty", idx))
		}
	}
	if len(c.RequestsTopic) == 0 {
		err = multierror.Append(err, errors.New("`requests-topic` should not be empty"))
	}
	if len(c.IndexName) == 0 {
		err = multierror.Append(err, errors.New("`index-name` should not be empty"))
	}
	if len(c.GroupID) == 0 {
		err = multierror.Append(err, errors.New("`group-id` should not be empty"))
	}
	if c.PollTimeoutMs <= 0 {
		err = multierror.Append(err, errors.New("`poll-timeout` should be positive"))
	}
	return err.ErrorOrNil()
}

func cloneConfigMap(m kafka.ConfigMap) kafka.ConfigMap {
	m2 := make(kafka.ConfigMap)
	for k, v := range m {
		m2[strings.Replace(k, "-", ".", -1)] = v
	}
	return m2
}
