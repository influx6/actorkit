package redis_test

import (
	"context"
	"testing"

	"github.com/gokit/actorkit/pubsubs/internal/encoders"
	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit/pubsubs/redis"

	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
)

func TestRedis(t *testing.T) {
	natspub, err := redis.NewPublisherSubscriberFactory(context.Background(), redis.Config{
		URL:         "",
		Marshaler:   encoders.NoAddressMarshaler{},
		Unmarshaler: encoders.NoAddressUnmarshaler{},
	})

	assert.NoError(t, err)
	assert.NotNil(t, natspub)

	factory := redis.PubSubFactory(func(factory *redis.PublisherSubscriberFactory, topic string) (pubsubs.Publisher, error) {
		return factory.Publisher(topic)
	}, func(factory *redis.PublisherSubscriberFactory, topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.Subscribe(topic, id, receiver)
	})(natspub)

	benches.PubSubFactoryTestSuite(t, factory)
}
