package segments_test

import (
	"context"
	"testing"

	"github.com/gokit/actorkit/internal"
	"github.com/gokit/actorkit/pubsubs/internal/encoders"
	"github.com/stretchr/testify/assert"

	segment "github.com/gokit/actorkit/pubsubs/kafka/segments"

	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
)

func TestNATS(t *testing.T) {
	kpub, err := segment.NewPublisherSubscriberFactory(context.Background(), segment.Config{
		URL:         "127.0.0.1:9092",
		ClusterID:   "cluster_server",
		ProjectID:   "wireco",
		Log:         &internal.TLog{},
		Marshaler:   segment.MarshalerWrapper{Envelope: encoders.NoAddressMarshaler{}},
		Unmarshaler: segment.UnmarshalerWrapper{Envelope: encoders.NoAddressUnmarshaler{}},
	})

	assert.NoError(t, err)
	assert.NotNil(t, kpub)

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
