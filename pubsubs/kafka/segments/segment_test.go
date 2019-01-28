package segments_test

import (
	"context"
	"testing"

	"github.com/gokit/actorkit/internal"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
	"github.com/gokit/actorkit/pubsubs/internal/encoders"
	segment "github.com/gokit/actorkit/pubsubs/kafka/segments"
	"github.com/stretchr/testify/require"
)

func TestSegments(t *testing.T) {
	kpub, err := segment.NewPublisherSubscriberFactory(context.Background(), segment.Config{
		Brokers:     []string{"127.0.0.1:9092"},
		Log:         &internal.TLog{},
		Marshaler:   segment.MarshalerWrapper{Envelope: encoders.NoAddressMarshaler{}},
		Unmarshaler: segment.UnmarshalerWrapper{Envelope: encoders.NoAddressUnmarshaler{}},
	})

	require.NoError(t, err)
	require.NotNil(t, kpub)

	if err != nil {
		return
	}

	defer kpub.Close()

	factory := segment.PubSubFactory(func(factory *segment.PublisherSubscriberFactory, topic string) (pubsubs.Publisher, error) {
		return factory.Publisher(topic)
	}, func(factory *segment.PublisherSubscriberFactory, topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.Subscribe(topic, id, receiver)
	}, func(factory *segment.PublisherSubscriberFactory, topic string, group string, id string, r pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.QueueSubscribe(topic, group, id, r)
	})(kpub)

	benches.PubSubFactoryTestSuite(t, factory)
}
