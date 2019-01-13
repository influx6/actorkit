package samsara_test

import (
	"context"
	"testing"

	"github.com/Shopify/sarama"

	"github.com/stretchr/testify/require"

	"github.com/gokit/actorkit/internal"

	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
	"github.com/gokit/actorkit/pubsubs/internal/encoders"
	"github.com/gokit/actorkit/pubsubs/kafka/samsara"
)

func TestSamaraPubsub(t *testing.T) {
	publishers, err := samsara.NewPublisherConsumerFactory(context.Background(), samsara.Config{
		Brokers:     []string{"127.0.0.1:9092"},
		ProjectID:   "wireco",
		Version:     sarama.V0_11_0_2,
		Log:         &internal.TLog{},
		Marshaler:   samsara.MarshalerWrapper{Envelope: encoders.NoAddressMarshaler{}},
		Unmarshaler: samsara.UnmarshalerWrapper{Envelope: encoders.NoAddressUnmarshaler{}},
	})

	require.NoError(t, err)
	require.NotNil(t, publishers)

	factory := samsara.PubSubFactory(func(factory *samsara.PublisherConsumerFactory, topic string) (pubsubs.Publisher, error) {
		return factory.NewPublisher(topic, nil)
	}, func(factory *samsara.PublisherConsumerFactory, topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.NewConsumer(topic, id, receiver, nil)
	}, func(factory *samsara.PublisherConsumerFactory, topic string, grp string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.NewGroupConsumer(topic, grp, id, receiver, nil)
	})(publishers)

	benches.PubSubFactoryTestSuite(t, factory)
}
