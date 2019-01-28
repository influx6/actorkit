package benches

import (
	"testing"
	"time"

	"github.com/gokit/actorkit"
	"github.com/gokit/actorkit/pubsubs"
	"github.com/stretchr/testify/require"
)

//**************************************************************************
// Tests Publishers and Subscribers
//**************************************************************************

// PubSubFactoryTestSuite verifies the giving behaviour of a giving provider of a pubsubs.PubSubFactories.
func PubSubFactoryTestSuite(t *testing.T, pubsub pubsubs.PubSubFactory) {
	t.Run("Publish Message Only", func(t *testing.T) {
		testMessagePublishing(t, pubsub)
	})

	t.Run("Publish and Subscribe", func(t *testing.T) {
		testMessagePublishingAndSubscription(t, pubsub)
	})
}

func testMessagePublishing(t *testing.T, pubsub pubsubs.PublisherFactory) {
	pub, err := pubsub.NewPublisher("rats")
	require.NoError(t, err)
	require.NotNil(t, pub)

	require.NoError(t, pub.Publish(actorkit.CreateEnvelope(actorkit.DeadLetters(), actorkit.Header{}, "300")))
	require.NoError(t, pub.Close())
}

func testMessagePublishingAndSubscription(t *testing.T, pubsub pubsubs.PubSubFactory) {
	pub, err := pubsub.NewPublisher("rats")
	require.NoError(t, err)
	require.NotNil(t, pub)

	rec := make(chan pubsubs.Message, 1)
	sub, err := pubsub.NewSubscriber("rats", "my-group", func(message pubsubs.Message) (pubsubs.Action, error) {
		rec <- message
		return pubsubs.ACK, nil
	})

	require.NoError(t, err)
	require.NotNil(t, sub)

	defer sub.Stop()

	require.NoError(t, pub.Publish(actorkit.CreateEnvelope(actorkit.DeadLetters(), actorkit.Header{}, "300")))

	select {
	case msg := <-rec:
		require.Equal(t, "rats", msg.Topic)
		require.NotNil(t, msg.Envelope.Data)
		require.Equal(t, "300", msg.Envelope.Data)
	case <-time.After(time.Minute * 1):
		require.Fail(t, "Should have successfully received published message")
	}

	require.NoError(t, pub.Close())
}

//**************************************************************************
// Benchmarks Publishers and Subscribers
//**************************************************************************
