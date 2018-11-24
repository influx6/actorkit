package es

import "sync"

//***********************************
//  EventStream
//***********************************

// Predicate enforces a filter on giving events for a Subscription.
type Predicate func(interface{}) bool

// EventHandler defines a function type for representing a handler for listing to events.
type EventHandler func(interface{})

// New returns a new instance implementing EventStream.
func New() *EventStream {
	return &EventStream{}
}

//EventStream implements a pub-sub system.
type EventStream struct {
	sl   sync.RWMutex
	subs []*Subscription
}

// Reset clears event stream subscription list.
func (es *EventStream) Reset() {
	es.sl.Lock()
	es.subs = nil
	es.sl.Unlock()
}

// Subscribe adds a new subscription with giving handler.
func (es *EventStream) Subscribe(e EventHandler) *Subscription {
	sub := new(Subscription)
	sub.src = es
	sub.handler = e

	es.sl.Lock()
	sub.index = len(es.subs)
	es.subs = append(es.subs, sub)
	es.sl.Unlock()
	return sub
}

// Publish publishes a value to all subscribers.
func (es *EventStream) Publish(b interface{}) {
	es.sl.RLock()
	defer es.sl.RUnlock()

	for _, sub := range es.subs {
		if sub.p != nil && !sub.p(b) {
			continue
		}
		sub.handler(b)
	}
}

// Unsubscribe removes giving Subscription.
func (es *EventStream) Unsubscribe(sb *Subscription) {
	es.sl.Lock()
	defer es.sl.Unlock()

	if sb.index == -1 {
		return
	}

	if es.subs == nil {
		return
	}

	total := len(es.subs)
	last := es.subs[total-1]

	// if the last item is the same and we only have 1 item.
	if last == sb && total == 1 {
		es.subs = nil
		sb.handler = nil
		sb.p = nil
		sb.index = -1
		return
	}

	// replace the Subscription with the last item.
	es.subs[sb.index] = last

	// set last index to previous index.
	last.index = sb.index

	// nil all references.
	sb.handler = nil
	sb.p = nil
	sb.index = -1

	// reduce es.subs slice header.
	es.subs = es.subs[:total-1]
}

// Subscription implements a structure to modify received events
// and to stop subscription to event.
type Subscription struct {
	index   int
	p       Predicate
	src     *EventStream
	handler EventHandler
}

// WithPredicate adds a predicate to filter the Subscription.
func (s *Subscription) WithPredicate(p Predicate) *Subscription {
	s.src.sl.Lock()
	s.p = p
	s.src.sl.Unlock()
	return s
}

// Stop ends the Subscription.
func (s *Subscription) Stop() {
	s.src.Unsubscribe(s)
}
