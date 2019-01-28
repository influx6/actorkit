package natstreaming_test

import (
	"context"
	"testing"

	"github.com/gokit/actorkit/internal"
	"github.com/gokit/actorkit/pubsubs/internal/encoders"
	"github.com/stretchr/testify/assert"

	streaming "github.com/gokit/actorkit/pubsubs/natstreaming"

	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
)

func TestNATS(t *testing.T) {
	natspub, err := streaming.NewPublisherSubscriberFactory(context.Background(), streaming.Config{
		URL:         "localhost:4222",
		ClusterID:   "cluster_server",
		ProjectID:   "wireco",
		Log:         &internal.TLog{},
		Marshaler:   encoders.NoAddressMarshaler{},
		Unmarshaler: encoders.NoAddressUnmarshaler{},
	})

	require.NoError(t, err)
	require.NotNil(t, natspub)

	if err != nil {
		return
	}

	defer natspub.Close()

	factory := streaming.PubSubFactory(func(factory *streaming.PublisherSubscriberFactory, topic string) (pubsubs.Publisher, error) {
		return factory.Publisher(topic)
	}, func(factory *streaming.PublisherSubscriberFactory, topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.Subscribe(topic, id, receiver, nil)
	}, func(factory *streaming.PublisherSubscriberFactory, topic string, group string, id string, r pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.QueueSubscribe(topic, group, id, r, nil)
	})(natspub)

	benches.PubSubFactoryTestSuite(t, factory)
}
