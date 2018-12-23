package benches

import (
	"testing"
	"time"

	"github.com/gokit/actorkit"

	"github.com/stretchr/testify/assert"

	"github.com/gokit/actorkit/transit"
)

//**************************************************************************
// Tests Publishers and Subscribers
//**************************************************************************

// PubSubFactoryTestSuite verifies the giving behaviour of a giving provider of a transit.PubSubFactories.
func PubSubFactoryTestSuite(t *testing.T, pubsub transit.PubSubFactory) {
	testMessagePublishing(t, pubsub)
	testMessagePublishingAndSubscription(t, pubsub)
}

func testMessagePublishing(t *testing.T, pubsub transit.PublisherFactory) {
	pub, err := pubsub.NewPublisher("rats")
	assert.NoError(t, err)
	assert.NotNil(t, pub)

	assert.NoError(t, pub.Publish(actorkit.CreateEnvelope(actorkit.DeadLetters(), actorkit.Header{}, "300")))

	<-time.After(3 * time.Second)
	assert.NoError(t, pub.Close())
}

func testMessagePublishingAndSubscription(t *testing.T, pubsub transit.PubSubFactory) {
	pub, err := pubsub.NewPublisher("rats")
	assert.NoError(t, err)
	assert.NotNil(t, pub)

	rec := make(chan transit.Message, 1)
	sub, err := pubsub.NewSubscriber("rats", "my-group", func(message transit.Message) error {
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
