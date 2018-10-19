package actorkit

import "sync"

// Watchers implements the Watchable interface.
type Watchers struct {
	wl       sync.RWMutex
	watchers map[string]func(interface{})
}

// NewWatchers returns a new instance of Watcher.
func NewWatchers() *Watchers {
	return &Watchers{
		watchers: map[string]func(interface{}){},
	}
}

// Inform delivers a giving value to all registered subscriers.
func (w *Watchers) Inform(k interface{}) {
	w.wl.RLock()
	defer w.wl.RUnlock()
	for _, wm := range w.watchers {
		wm(k)
	}
}

// RemoveWatchers removes all registered subscribers from watcher.
func (w *Watchers) RemoveWatchers() {
	w.wl.Lock()
	defer w.wl.Unlock()
	w.watchers = map[string]func(interface{}){}
}

// RemoveWatcher disconnects giving subscription function for giving Addr.
func (w *Watchers) RemoveWatcher(m Addr) {
	w.wl.Lock()
	defer w.wl.Unlock()
	if _, ok := w.watchers[m.ID()]; !ok {
		delete(w.watchers, m.ID())
	}
}

// AddWatcher adds giving function as subscription function to
// watch list for giving Addr.
func (w *Watchers) AddWatcher(m Addr, fn func(interface{})) {
	w.wl.Lock()
	defer w.wl.Unlock()
	if _, ok := w.watchers[m.ID()]; !ok {
		w.watchers[m.ID()] = fn
	}
}
