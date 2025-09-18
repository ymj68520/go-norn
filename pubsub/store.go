package pubsub

import (
	"errors"
)

// EventStore 简单接口，用于持久化事件（LevelDB 实现建议）
type EventStore interface {
	Append(e *Event) error
	GetRange(fromSeq uint64, toSeq uint64) ([]*Event, error)
	LatestSeq() (uint64, error)
	Close() error
}

// 简单的内存实现（供测试）
type MemStore struct {
	events []*Event
}

func NewMemStore() *MemStore {
	return &MemStore{events: make([]*Event, 0)}
}

func (m *MemStore) Append(e *Event) error {
	if e == nil {
		return errors.New("nil event")
	}
	m.events = append(m.events, e)
	return nil
}

func (m *MemStore) GetRange(fromSeq uint64, toSeq uint64) ([]*Event, error) {
	var res []*Event
	for _, ev := range m.events {
		if ev.Seq >= fromSeq && (toSeq == 0 || ev.Seq <= toSeq) {
			res = append(res, ev)
		}
	}
	return res, nil
}

func (m *MemStore) LatestSeq() (uint64, error) {
	if len(m.events) == 0 {
		return 0, nil
	}
	return m.events[len(m.events)-1].Seq, nil
}

func (m *MemStore) Close() error { return nil }
