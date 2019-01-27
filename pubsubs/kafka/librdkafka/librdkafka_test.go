package librdkafka_test

import (
	"context"
	"testing"

	"github.com/gokit/actorkit/internal"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
	"github.com/gokit/actorkit/pubsubs/internal/encoders"
	kafka "github.com/gokit/actorkit/pubsubs/kafka/librdkafka"
	"github.com/stretchr/testify/require"
)

func TestKafka(t *testing.T) {
	publishers, err := kafka.NewPublisherConsumerFactory(context.Background(), kafka.Config{
		Brokers:     []string{"localhost:9092"},
		ProjectID:   "wireco",
		Log:         &internal.TLog{},
		Marshaler:   kafka.MarshalerWrapper{Envelope: encoders.NoAddressMarshaler{}},
		Unmarshaler: kafka.UnmarshalerWrapper{Envelope: encoders.NoAddressUnmarshaler{}},
	})

	require.NoError(t, err)
	require.NotNil(t, publishers)

	factory := kafka.PubSubFactory(func(factory *kafka.PublisherConsumerFactory, topic string) (pubsubs.Publisher, error) {
		return factory.NewPublisher(topic, nil)
	}, func(factory *kafka.PublisherConsumerFactory, topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.NewConsumer(topic, id, receiver)
	}, func(factory *kafka.PublisherConsumerFactory, topic string, grp string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.NewGroupConsumer(topic, grp, id, receiver)
	})(publishers)

	benches.PubSubFactoryTestSuite(t, factory)
}
