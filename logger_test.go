package actorkit_test

import (
	"testing"

	"github.com/gokit/actorkit"
	"github.com/stretchr/testify/assert"
)

func TestGetLogEvent(t *testing.T) {
	t.Run("basic fields", func(t *testing.T) {
		event := actorkit.LogMsg("My log")
		event.String("name", "thunder")
		event.Int("id", 234)
		assert.Equal(t, "{\"message\": \"My log\", \"name\": \"thunder\", \"id\": 234}", event.Message())
	})

	t.Run("with JSON fields", func(t *testing.T) {
		event := actorkit.LogMsg("My log")
		event.String("name", "thunder")
		event.Int("id", 234)
		event.ObjectJSON("data", map[string]interface{}{"id": 23})
		assert.Equal(t, "{\"message\": \"My log\", \"name\": \"thunder\", \"id\": 234, \"data\": {\"id\":23}}", event.Message())
	})

	t.Run("with Entry fields", func(t *testing.T) {
		event := actorkit.LogMsg("My log")
		event.String("name", "thunder")
		event.Int("id", 234)
		event.Object("data", func(event actorkit.LogEvent) {
			event.Int("id", 23)
		})
		assert.Equal(t, "{\"message\": \"My log\", \"name\": \"thunder\", \"id\": 234, \"data\": {\"id\": 23}}", event.Message())
	})

	t.Run("with bytes fields", func(t *testing.T) {
		event := actorkit.LogMsg("My log")
		event.String("name", "thunder")
		event.Int("id", 234)
		event.Bytes("data", []byte("{\"id\": 23}"))
		assert.Equal(t, "{\"message\": \"My log\", \"name\": \"thunder\", \"id\": 234, \"data\": {\"id\": 23}}", event.Message())
	})

	t.Run("using context fields", func(t *testing.T) {
		event := actorkit.LogMsgWithContext("My log", "data", nil)
		event.String("name", "thunder")
		event.Int("id", 234)
		assert.Equal(t, "{\"message\": \"My log\", \"data\": {\"name\": \"thunder\", \"id\": 234}}", event.Message())
	})

	t.Run("using context fields with hook", func(t *testing.T) {
		event := actorkit.LogMsgWithContext("My log", "data", func(event actorkit.LogEvent) {
			event.Bool("w", true)
		})

		event.String("name", "thunder")
		event.Int("id", 234)
		assert.Equal(t, "{\"message\": \"My log\", \"w\": true, \"data\": {\"name\": \"thunder\", \"id\": 234}}", event.Message())
	})
}

func BenchmarkGetLogEvent(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	b.Run("basic fields", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := b.N; i > 0; i-- {
			event := actorkit.LogMsg("My log")
			event.String("name", "thunder")
			event.Int("id", 234)
			event.Message()
		}
	})

	b.Run("with JSON fields", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := b.N; i > 0; i-- {
			event := actorkit.LogMsg("My log")
			event.String("name", "thunder")
			event.Int("id", 234)
			event.ObjectJSON("data", map[string]interface{}{"id": 23})
			event.Message()
		}
	})

	b.Run("with Entry fields", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := b.N; i > 0; i-- {
			event := actorkit.LogMsg("My log")
			event.String("name", "thunder")
			event.Int("id", 234)
			event.Object("data", func(event actorkit.LogEvent) {
				event.Int("id", 23)
			})
			event.Message()
		}
	})

	b.Run("with bytes fields", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := b.N; i > 0; i-- {
			event := actorkit.LogMsg("My log")
			event.String("name", "thunder")
			event.Int("id", 234)
			event.Bytes("data", []byte("{\"id\": 23}"))
			event.Message()
		}
	})
}
