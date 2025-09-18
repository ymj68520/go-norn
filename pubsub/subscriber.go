package pubsub

import (
	"sync"
)

type FilterFunc func(e *Event) bool

type Subscriber struct {
	id     string
	ch     chan *Event
	filter FilterFunc
	closed bool
	mu     sync.Mutex
}

func newSubscriber(id string, filter FilterFunc, buffer int) *Subscriber {
	return &Subscriber{
		id:     id,
		ch:     make(chan *Event, buffer),
		filter: filter,
	}
}

func (s *Subscriber) ID() string {
	return s.id
}

func (s *Subscriber) Events() <-chan *Event {
	return s.ch
}

func (s *Subscriber) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	close(s.ch)
	s.closed = true
}

func (s *Subscriber) send(e *Event) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	select {
	case s.ch <- e:
		return true
	default:
		// buffer full
		return false
	}
}
