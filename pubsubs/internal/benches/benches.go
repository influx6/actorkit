package benches

import (
	"testing"
	"time"

	"github.com/gokit/actorkit"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit/pubsubs"
)

//**************************************************************************
// Tests Publishers and Subscribers
//**************************************************************************

// PubSubFactoryTestSuite verifies the giving behaviour of a giving provider of a pubsubs.PubSubFactories.
func PubSubFactoryTestSuite(t *testing.T, pubsub pubsubs.PubSubFactory) {
	testMessagePublishing(t, pubsub)
	testMessagePublishingAndSubscription(t, pubsub)
}

func testMessagePublishing(t *testing.T, pubsub pubsubs.PublisherFactory) {
	pub, err := pubsub.NewPublisher("rats")
	assert.NoError(t, err)
	assert.NotNil(t, pub)

	assert.NoError(t, pub.Publish(actorkit.CreateEnvelope(actorkit.DeadLetters(), actorkit.Header{}, "300")))

	assert.NoError(t, pub.Close())
}

func testMessagePublishingAndSubscription(t *testing.T, pubsub pubsubs.PubSubFactory) {
	pub, err := pubsub.NewPublisher("rats")
	assert.NoError(t, err)
	assert.NotNil(t, pub)

	rec := make(chan pubsubs.Message, 1)
	sub, err := pubsub.NewSubscriber("rats", "my-group", func(message pubsubs.Message) error {
		rec <- message
		return nil
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

//**************************************************************************
// Benchmarks Publishers and Subscribers
//**************************************************************************
