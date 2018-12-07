package redis_test

import (
	"context"
	"testing"

	"github.com/gokit/actorkit/transit/internal/encoders"
	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit/transit/redis"

	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/transit"
	"github.com/gokit/actorkit/transit/internal/benches"
)

func TestRedis(t *testing.T) {
	natspub, err := redis.NewPublisherSubscriberFactory(context.Background(), redis.Config{
		URL:         "",
		Marshaler:   encoders.NoAddressMarshaler{},
		Unmarshaler: encoders.NoAddressUnmarshaler{},
	})

	assert.NoError(t, err)
	assert.NotNil(t, natspub)

	factory := redis.PubSubFactory(func(factory *redis.PublisherSubscriberFactory, topic string) (transit.Publisher, error) {
		return factory.Publisher(topic)
	}, func(factory *redis.PublisherSubscriberFactory, topic string, id string, receiver transit.Receiver) (actorkit.Subscription, error) {
		return factory.Subscribe(topic, id, receiver)
	})(natspub)

	benches.PubSubFactoryTestSuite(t, factory)
}
