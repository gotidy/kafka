package kafka

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"golang.org/x/net/context"
)

type Subscriber struct {
	client sarama.ConsumerGroup

	onError func(err error)

	wg *sync.WaitGroup
}

// NewSubscriber will initiate a the experimental Kafka consumer.
func NewSubscriber(group string, cfg *SubscriberConfig) (*Subscriber, error) {
	if group == "" {
		return nil, errors.New("group name is required")
	}

	saramaConfig, err := cfg.config()
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewConsumerGroup(cfg.BrokerHosts, group, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating consumer group client: %w", err)
	}

	subscriber := &Subscriber{client: client, wg: cfg.WaitGroup, onError: cfg.OnError}

	// Track errors
	go func() {
		for err := range client.Errors() {
			subscriber.onError(err)
		}
	}()

	return subscriber, nil
}

type consumerGroupHandler struct {
	handler func(ctx context.Context, msg MessageDone)
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.handler(session.Context(), ConsumerMessage{Message: msg, Session: session})
		// session.MarkMessage(msg, "")
	}
	return nil
}

func (s *Subscriber) Subscribe(ctx context.Context, handler func(ctx context.Context, msg MessageDone), topics ...string) {
	s.wg.Add(1)
	go func() {
		for {
			handler := consumerGroupHandler{handler: handler}
			err := s.client.Consume(ctx, topics, handler)
			if ctx.Err() != nil {
				break
			}
			if err != nil {
				s.onError(err)
			}
		}
		s.wg.Done()
	}()
}

func (s *Subscriber) Close() {
	go func() {
		s.wg.Wait()
		s.client.Close()
	}()
}

// GetPartitions is a helper function to look up which partitions are available
// via the given brokers for the given topic. This should be called only on startup.
func GetPartitions(brokerHosts []string, topic string) (partitions []int32, err error) {
	if len(brokerHosts) == 0 {
		return partitions, errors.New("at least 1 broker host is required")
	}

	if len(topic) == 0 {
		return partitions, errors.New("topic name is required")
	}

	var client sarama.Consumer
	client, err = sarama.NewConsumer(brokerHosts, sarama.NewConfig())
	if err != nil {
		return partitions, err
	}

	defer func() {
		if cerr := client.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	return client.Partitions(topic)
}
