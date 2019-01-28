package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/gokit/actorkit/pubsubs/internal/benches"
	"github.com/gokit/actorkit/pubsubs/internal/encoders"
	"github.com/gokit/actorkit/pubsubs/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedis(t *testing.T) {
	redispub, err := redis.NewPublisherSubscriberFactory(context.Background(), redis.Config{
		URL:         "localhost:6379",
		Marshaler:   encoders.NoAddressMarshaler{},
		Unmarshaler: encoders.NoAddressUnmarshaler{},
	})

	assert.NoError(t, err)
	assert.NotNil(t, redispub)

	factory := redis.PubSubFactory(func(factory *redis.PublisherSubscriberFactory, topic string) (pubsubs.Publisher, error) {
		return factory.Publisher(topic)
	}, func(factory *redis.PublisherSubscriberFactory, topic string, id string, receiver pubsubs.Receiver) (pubsubs.Subscription, error) {
		return factory.Subscribe(topic, id, receiver)
	})(redispub)

	benches.PubSubFactoryTestSuite(t, factory)
}

func TestRedisPurely(t *testing.T) {
	redispub, err := redis.NewPublisherSubscriberFactory(context.Background(), redis.Config{
		URL:         "localhost:6379",
		Marshaler:   encoders.NoAddressMarshaler{},
		Unmarshaler: encoders.NoAddressUnmarshaler{},
	})

	assert.NoError(t, err)
	require.NotNil(t, redispub)

	pub, err := redispub.Publisher("rats")
	assert.NoError(t, err)
	assert.NotNil(t, pub)

	rec := make(chan pubsubs.Message, 1)
	sub, err := redispub.Subscribe("rats", "my-group", func(message pubsubs.Message) (pubsubs.Action, error) {
		rec <- message
		return pubsubs.ACK, nil
	})

	assert.NoError(t, err)
	assert.NotNil(t, sub)

	defer sub.Stop()

	assert.NoError(t, pub.Publish(actorkit.CreateEnvelope(actorkit.DeadLetters(), actorkit.Header{}, "300")))

	select {
	case msg := <-rec:
		assert.Equal(t, "rats", msg.Topic)
		assert.NotNil(t, msg.Envelope.Data)
		assert.Equal(t, "300", msg.Envelope.Data)
	case <-time.After(time.Second * 5):
		assert.Fail(t, "Should have successfully received published message")
	}

	assert.NoError(t, pub.Close())
}
