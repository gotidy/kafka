package kafka

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
)

// RequiredAcks is used in Produce Requests to tell the broker how many replica acknowledgements
// it must see before responding. Any of the constants defined here are valid. On broker versions
// prior to 0.8.2.0 any other positive int16 is also valid (the broker will wait for that many
// acknowledgements) but in 0.8.2.0 and later this will raise an exception (it has been replaced
// by setting the `min.isr` value in the brokers configuration).
type RequiredAcks = sarama.RequiredAcks

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = sarama.NoResponse
	// WaitForLocal waits for only the local commit to succeed before responding.
	WaitForLocal RequiredAcks = sarama.WaitForLocal
	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll RequiredAcks = sarama.WaitForAll
)

type BalanceStrategy = sarama.BalanceStrategy

func StringToBalanceStrategy(strategy string) (BalanceStrategy, error) {
	switch strategy {
	case "sticky":
		return sarama.BalanceStrategySticky, nil
	case "roundrobin":
		return sarama.BalanceStrategyRoundRobin, nil
	case "range":
		return sarama.BalanceStrategyRange, nil
	default:
		return nil, fmt.Errorf("unrecognized consumer group partition balance strategy: %s", strategy)
	}
}

var (
	BalanceStrategySticky     = sarama.BalanceStrategySticky
	BalanceStrategyRoundRobin = sarama.BalanceStrategyRoundRobin
	BalanceStrategyRange      = sarama.BalanceStrategyRange
)

// Config holds the basic information for working with Kafka.
type Config struct {
	BrokerHosts []string
	Version     string

	User     string
	Password string

	WaitGroup *sync.WaitGroup
}

func newConfig() Config {
	return Config{WaitGroup: &sync.WaitGroup{}}
}

type SubscriberConfig struct {
	Config

	// Start from oldest offset
	Oldest          bool
	BalanceStrategy BalanceStrategy

	OnError func(err error)
}

func NewSubscriberConfig() *SubscriberConfig {
	return &SubscriberConfig{
		Config:          newConfig(),
		BalanceStrategy: BalanceStrategyRoundRobin,
	}
}

func (c *SubscriberConfig) config() (*sarama.Config, error) {
	config, err := toConfig(&c.Config)
	if err != nil {
		return nil, err
	}

	config.Consumer.Group.Rebalance.Strategy = c.BalanceStrategy
	if c.OnError != nil {
		config.Consumer.Return.Errors = true
	}
	if c.Oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	return config, nil
}

type PublisherConfig struct {
	Config

	MaxRetry     int
	RequiredAcks RequiredAcks

	OnMessageError   func(err error, msg Message)
	OnMessageSucceed func(msg Message)
}

func NewPublisherConfig() *PublisherConfig {
	return &PublisherConfig{
		Config:   newConfig(),
		MaxRetry: 3,
	}
}

func (c *PublisherConfig) config() (*sarama.Config, error) {
	config, err := toConfig(&c.Config)
	if err != nil {
		return nil, err
	}

	config.Producer.RequiredAcks = c.RequiredAcks
	if c.MaxRetry != 0 {
		config.Producer.Retry.Max = c.MaxRetry
	}
	if c.OnMessageError != nil {
		config.Producer.Return.Errors = true
	}
	if c.OnMessageSucceed != nil {
		config.Producer.Return.Successes = true
	}
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	return config, nil
}

func toConfig(cfg *Config) (*sarama.Config, error) {
	if len(cfg.BrokerHosts) == 0 {
		return nil, errors.New("at least 1 broker host is required")
	}

	config := sarama.NewConfig()

	config.Net.SASL.User = cfg.User
	config.Net.SASL.Password = cfg.Password
	version, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		return nil, fmt.Errorf("error parsing Kafka version: %w", err)
	}
	config.Version = version

	return config, nil
}
