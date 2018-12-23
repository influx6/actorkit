package actorkit

import "github.com/gokit/es"

// Eventer implements the EventStream interface by decorating
// the gokit es event implementation.
type Eventer struct {
	es *es.EventStream
}

// NewEventer returns a instance of a Eventer.
func NewEventer() *Eventer {
	return &Eventer{es: es.New()}
}

// EventWith returns a instance of a Eventer using provided es.EventStream.
func EventerWith(em *es.EventStream) *Eventer {
	return &Eventer{es: em}
}

// Reset resets the underline event subscription list.
func (e Eventer) Reset() {
	e.es.Reset()
}

// Publish publishes a giving message.
func (e Eventer) Publish(m interface{}) {
	e.es.Publish(m)
}

// Subscribe adds a giving subscription using the provided handler and predicate.
func (e Eventer) Subscribe(handler Handler, predicate Predicate) Subscription {
	return e.es.Subscribe(func(m interface{}) {
		handler(m)
	}).WithPredicate(func(m interface{}) bool {
		if predicate == nil {
			return true
		}
		return predicate(m)
	})
}
