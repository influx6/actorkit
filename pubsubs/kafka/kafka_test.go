package kafka_test

import (
	"testing"

	coKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
	"github.com/gokit/actorkit/pubsubs/internal/encoders"
	"github.com/gokit/actorkit/pubsubs/kafka"
)

func TestKafka(t *testing.T) {
	publishers := kafka.NewPublisherFactory([]string{}, nil, &kafka.KAMarshaler{
		Envelope: encoders.NoAddressMarshaler{},
	}, nil)

	subscribers := kafka.NewConsumerFactory(kafka.Config{
		Brokers: []string{},
	}, &kafka.KAUnmarshaler{Envelope: encoders.NoAddressUnmarshaler{}})

	factory := kafka.PubSubFactory(func(factory *kafka.PublisherFactory, topic string) (pubsubs.Publisher, error) {
		return factory.NewPublisher(topic, coKafka.ConfigMap{})
	}, func(factory *kafka.ConsumerFactory, topic string, id string, receiver pubsubs.Receiver) (actorkit.Subscription, error) {
		return factory.CreateConsumer(topic, id, receiver, func(e error) kafka.Directive {
			return kafka.Rollback
		})
	})(publishers, subscribers)

	benches.PubSubFactoryTestSuite(t, factory)
}
