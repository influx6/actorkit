package es

import "sync"

//***********************************
//  EventStream
//***********************************

// Predicate enforces a filter on giving events for a subscription.
type Predicate func(interface{}) bool

// EventHandler defines a function type for representing a handler for listing to events.
type EventHandler func(interface{})

// Subscription defines a subscription for an event.
type Subscription interface{
	Stop()
	WithPredicate(Predicate) Subscription
}

// EventStream defines interface for pubsub.
type EventStream interface{
	Publish(interface{})
	Subscribe(handler EventHandler) Subscription
}

// New returns a new instance implementing EventStream.
func New() EventStream{
	return &eventStream{}
}

type eventStream struct{
	sl sync.RWMutex
	subs []*subscription
}


func (es *eventStream) Subscribe(e EventHandler) Subscription{
	sub := new(subscription)
	sub.src = es
	sub.handler = e

	es.sl.Lock()
	sub.index = len(es.subs)
	es.subs = append(es.subs, sub)
	es.sl.Unlock()
	return sub
}

func (es *eventStream) Publish(b interface{}){
	es.sl.RLock()
	defer es.sl.RUnlock()

	for _, sub := range es.subs {
		if sub.p != nil && !sub.p(b){
			continue
		}
		sub.handler(b)
	}
}

func (es *eventStream) Unsubscribe(sb *subscription){
	if sb.index == -1 {return}

	es.sl.Lock()
	defer es.sl.Unlock()

	total := len(es.subs)
	last := es.subs[total-1]

	// if the last item is the same and we only have 1 item.
	if last == sb && total == 1{
		es.subs = nil
		sb.handler = nil
		sb.p = nil
		sb.index = -1
		return
	}

	// replace the subscription with the last item.
	es.subs[sb.index] = last

	// set last index to previous index.
	last.index = sb.index

	// nil all references.
	sb.handler = nil
	sb.p = nil
	sb.index = -1

	// reduce es.subs slice header.
	es.subs = es.subs[:total - 1]
}

type subscription struct{
	index int
	p Predicate
	src *eventStream
	handler EventHandler
}

// WithPredicate adds a predicate to filter the subscription.
func (s *subscription) WithPredicate(p Predicate) Subscription{
	s.src.sl.Lock()
	s.p = p
	s.src.sl.Unlock()
	return s
}

// Stop ends the subscription.
func (s *subscription) Stop(){
	s.src.Unsubscribe(s)
}
