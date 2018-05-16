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

func (w *Watchers) Inform(k interface{}) {
	w.wl.RLock()
	defer w.wl.RUnlock()
	for _, wm := range w.watchers {
		wm(k)
	}
}

func (w *Watchers) RemoveWatchers() {
	w.wl.Lock()
	defer w.wl.Unlock()
	w.watchers = map[string]func(interface{}){}
}

func (w *Watchers) RemoveWatcher(m Mask) {
	w.wl.Lock()
	defer w.wl.Unlock()
	if _, ok := w.watchers[m.ID()]; !ok {
		delete(w.watchers, m.ID())
	}
}

func (w *Watchers) AddWatcher(m Mask, fn func(interface{})) {
	w.wl.Lock()
	defer w.wl.Unlock()
	if _, ok := w.watchers[m.ID()]; !ok {
		w.watchers[m.ID()] = fn
	}
}
