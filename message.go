package kafka

import "github.com/Shopify/sarama"

type Message interface {
	Topic() string
	Key() []byte
	Value() []byte
}

type Done interface {
	Done() error
}

type MessageDone interface {
	Message
	Done
}

type ConsumerMessage struct {
	Message *sarama.ConsumerMessage
	Session sarama.ConsumerGroupSession
}

// Topic will return the message topic.
func (m ConsumerMessage) Topic() string {
	return m.Message.Topic
}

// Message will return the message payload.
func (m ConsumerMessage) Value() []byte {
	return m.Message.Value
}

// Key will return the message key.
func (m ConsumerMessage) Key() []byte {
	return m.Message.Key
}

// Done will emit the message's offset.
func (m ConsumerMessage) Done() error {
	m.Session.MarkMessage(m.Message, "")
	return nil
}

type ProducerMessage struct {
	Message *sarama.ProducerMessage
}

// Topic will return the message topic.
func (m ProducerMessage) Topic() string {
	return m.Message.Topic
}

// Value will return the message payload.
func (m ProducerMessage) Value() []byte {
	b, _ := m.Message.Value.Encode()
	return b
}

// Key will return the message key.
func (m ProducerMessage) Key() []byte {
	b, _ := m.Message.Key.Encode()
	return b
}
