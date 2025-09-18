package pubsub

import (
	"errors"
	"sync"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

const (
	connPoolSize = 128
)

// Publisher 负责把事件发到本地 router 或者广播到网络
type Publisher struct {
	conns []*websocket.Conn
	count int

	lock   sync.RWMutex
	router *Router
	// TODO: 注入 p2p/broadcast 客户端接口以便 network broadcast
}

func NewPublisher(r *Router) *Publisher {
	return &Publisher{router: r}
}

// PublishLocal 发布到本地 router
func (p *Publisher) PublishLocal(e *Event) error {
	if p.router == nil {
		return errors.New("no router")
	}
	return p.router.Publish(e)
}

// BroadcastToNetwork 将事件广播到 p2p（需要实现 p2p 客户端）
func (p *Publisher) BroadcastToNetwork(e *Event) error {
	// TODO: serialize 并通过 p2p 发布
	return errors.New("network broadcast not implemented")
}

func CreateNewEventPublisher() *Publisher {
	publisher := Publisher{
		conns: make([]*websocket.Conn, connPoolSize),
		count: 0,
	}

	return &publisher
}

func (e *Publisher) Full() bool {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.count >= connPoolSize
}

func (e *Publisher) Publish(data []byte) {
	e.lock.Lock()
	defer e.lock.Unlock()

	for idx, conn := range e.conns {
		if conn == nil {
			continue
		}

		err := conn.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			log.WithError(err).Errorln("Write message to conn failed.")
			e.conns[idx] = nil
			e.count--
			continue
		}
	}
}

func (e *Publisher) AppendNewConnection(conn *websocket.Conn) {
	e.lock.Lock()
	defer e.lock.Unlock()

	pos := e.SelectPosition()
	if pos != -1 {
		e.conns[pos] = conn
		e.count++
	}
}

func (e *Publisher) SelectPosition() int {
	for idx := range e.conns {
		if e.conns[idx] == nil {
			return idx
		}
	}

	return -1
}
