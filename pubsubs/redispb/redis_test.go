package redispb_test

import (
	"context"
	"testing"
	"time"

	"github.com/gokit/actorkit/internal"

	"github.com/go-redis/redis"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
	"github.com/gokit/actorkit/pubsubs/internal/encoders"
	"github.com/gokit/actorkit/pubsubs/redispb"
	"github.com/stretchr/testify/require"
)

func TestRedis(t *testing.T) {
	redispub, err := redispb.NewPublisherSubscriberFactory(context.Background(), redispb.Config{
		Host: &redis.Options{
			Network:     "tcp",
			Addr:        "0.0.0.0:6379",
			DialTimeout: time.Second * 3,
		},
		Log:         &internal.TLog{},
		Marshaler:   encoders.NoAddressMarshaler{},
		Unmarshaler: encoders.NoAddressUnmarshaler{},
	})

	require.NoError(t, err)
	require.NotNil(t, redispub)

	factory := redispb.PubSubFactory(func(factory *redispb.PublisherSubscriberFactory, topic string) (pubsubs.Publisher, error) {
		return factory.Publisher(topic)
	}, func(factory *redispb.PublisherSubscriberFactory, topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.Subscribe(topic, id, receiver)
	})(redispub)

	benches.PubSubFactoryTestSuite(t, factory)
}
