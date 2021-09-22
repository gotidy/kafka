package kafka

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
	"golang.org/x/net/context"
)

type Bytes = []byte

// SyncPublisher is an experimental publisher that provides an implementation for
// Kafka using the Shopify/sarama library.
type SyncPublisher struct {
	client sarama.SyncProducer
}

// NewPublisher will initiate a new experimental Kafka publisher.
func NewSyncPublisher(cfg *PublisherConfig) (*SyncPublisher, error) {
	var err error
	p := &SyncPublisher{}

	saramaConfig, err := cfg.config()
	if err != nil {
		return nil, err
	}

	p.client, err = sarama.NewSyncProducer(cfg.BrokerHosts, saramaConfig)
	return p, err
}

// Publish will marshal the proto message and emit it to the Kafka topic.
func (p *SyncPublisher) Publish(ctx context.Context, topic string, key []byte, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}
	return p.PublishRaw(ctx, topic, key, data)
}

// PublishRaw will emit the byte array to the Kafka topic.
func (p *SyncPublisher) PublishRaw(_ context.Context, topic string, key []byte, data []byte) error {
	if len(topic) == 0 {
		return errors.New("topic name is required")
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(data),
	}
	_, _, err := p.client.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}
	return nil
}

// Close will close the pub connection.
func (p *SyncPublisher) Close() error {
	return p.client.Close()
}

type AsyncPublisher struct {
	client sarama.AsyncProducer
}

// NewPublisher will initiate a new experimental Kafka publisher.
func NewAsyncPublisher(cfg *PublisherConfig) (*AsyncPublisher, error) {
	var err error
	p := &AsyncPublisher{}

	saramaConfig, err := cfg.config()
	if err != nil {
		return nil, err
	}

	p.client, err = sarama.NewAsyncProducer(cfg.BrokerHosts, saramaConfig)
	if err != nil {
		return nil, err
	}

	cfg.WaitGroup.Add(1)
	go func() {
		onError := cfg.OnMessageError
		if onError == nil {
			onError = func(_ error, _ Message) {}
		}
		for err := range p.client.Errors() {
			if err.Msg == nil {
				onError(err, nil)
			} else {
				onError(err, ProducerMessage{Message: err.Msg})
			}
		}
		cfg.WaitGroup.Done()
	}()

	cfg.WaitGroup.Add(1)
	go func() {
		onSucceed := cfg.OnMessageSucceed
		if onSucceed == nil {
			onSucceed = func(_ Message) {}
		}
		for msg := range p.client.Successes() {
			onSucceed(ProducerMessage{Message: msg})
		}
		cfg.WaitGroup.Done()
	}()

	return p, nil
}

// Publish will marshal the message and emit it to the Kafka topic.
func (p *AsyncPublisher) Publish(ctx context.Context, topic string, key []byte, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}
	return p.PublishRaw(ctx, topic, key, data)
}

// PublishRaw will emit the byte array to the Kafka topic.
func (p *AsyncPublisher) PublishRaw(ctx context.Context, topic string, key []byte, data []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(data),
	}

	return p.publish(ctx, msg)
}

// PublishRaw will emit the byte array to the Kafka topic.
func (p *AsyncPublisher) publish(ctx context.Context, msg *sarama.ProducerMessage) error {
	if msg.Topic == "" {
		return errors.New("topic name is required")
	}

	select {
	case p.client.Input() <- msg:
	case <-ctx.Done():
	}

	return nil
}

// PublishRaw will emit the byte array to the Kafka topic.
func (p *AsyncPublisher) Retry(ctx context.Context, msg Message) error {
	var message *sarama.ProducerMessage

	if msg, ok := msg.(ProducerMessage); ok {
		message = msg.Message
	} else {
		message = &sarama.ProducerMessage{
			Topic: msg.Topic(),
			Key:   sarama.ByteEncoder(msg.Key()),
			Value: sarama.ByteEncoder(msg.Value()),
		}
	}

	return p.publish(ctx, message)
}

// Close will close the pub connection.
func (p *AsyncPublisher) Close() error {
	p.client.AsyncClose()
	return nil
}
