package v1

import (
	"sync"
)

type subscriptions struct {
	mu sync.RWMutex
	m  map[byte]*subscription
}

func newSubscriptions() *subscriptions {
	return &subscriptions{
		m: make(map[byte]*subscription),
	}
}

func (sm *subscriptions) add(s *subscription) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.m[s.id] = s
}

func (sm *subscriptions) get(id byte) (*subscription, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	s, ok := sm.m[id]
	return s, ok
}

func (sm *subscriptions) delete(id byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.m, id)
}

func (sm *subscriptions) close() {
	for _, sub := range sm.m {
		sub.Close()
	}
}
