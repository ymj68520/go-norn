package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

type QueueFullPolicy int
type EventTopic string

const (
	QueueBlock QueueFullPolicy = iota
	QueueDropNewest
	QueueDropOldest
)

type RouterConfig struct {
	QueueSize       int
	DispatchWorkers int
	SubBuffer       int
	QueuePolicy     QueueFullPolicy
	ReplayOnStart   bool
}

type Router struct {
	cfg         RouterConfig
	queue       chan *Event
	subs        map[string]*Subscriber
	publishMap  map[EventTopic]*Publisher
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	nextSeq     uint64
	store       EventStore // optional persistence
	started     bool
	startStopMu sync.RWMutex
}

var (
	routerOnce = sync.Once{}
	routerInst *Router
	upgrader   = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
)

// NewRouter 返回新 Router，cfg 可为 nil 使用默认值
func NewRouter(cfg *RouterConfig, store EventStore) *Router {
	c := RouterConfig{
		QueueSize:       1024,
		DispatchWorkers: 1,
		SubBuffer:       256,
		QueuePolicy:     QueueDropNewest,
		ReplayOnStart:   false,
	}
	if cfg != nil {
		c = *cfg
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Router{
		cfg:    c,
		queue:  make(chan *Event, c.QueueSize),
		subs:   make(map[string]*Subscriber),
		ctx:    ctx,
		cancel: cancel,
		store:  store,
	}
	return r
}

// Start 启动 dispatcher
func (r *Router) Start() error {
	r.startStopMu.Lock()
	defer r.startStopMu.Unlock()
	if r.started {
		return nil
	}
	r.started = true
	// 回放
	if r.cfg.ReplayOnStart && r.store != nil {
		if seq, err := r.store.LatestSeq(); err == nil {
			_ = seq // 可选择回放逻辑
		}
	}
	// 启动 dispatcher
	for i := 0; i < r.cfg.DispatchWorkers; i++ {
		r.wg.Add(1)
		go r.dispatcher()
	}
	return nil
}

// Stop 停止 router
func (r *Router) Stop() {
	r.startStopMu.Lock()
	if !r.started {
		r.startStopMu.Unlock()
		return
	}
	r.started = false
	r.startStopMu.Unlock()

	r.cancel()
	r.wg.Wait()
	// close subscribers
	r.mu.Lock()
	for _, s := range r.subs {
		s.close()
	}
	r.subs = make(map[string]*Subscriber)
	r.mu.Unlock()
	if r.store != nil {
		_ = r.store.Close()
	}
}

// Subscribe 注册订阅者，返回 id 与 subscriber
func (r *Router) Subscribe(filter FilterFunc, buffer int) *Subscriber {
	if buffer <= 0 {
		buffer = r.cfg.SubBuffer
	}
	id := uuidNew()
	s := newSubscriber(id, filter, buffer)
	r.mu.Lock()
	r.subs[id] = s
	r.mu.Unlock()
	return s
}

// Unsubscribe
func (r *Router) Unsubscribe(id string) {
	r.mu.Lock()
	if s, ok := r.subs[id]; ok {
		s.close()
		delete(r.subs, id)
	}
	r.mu.Unlock()
}

// Publish 将事件入队
func (r *Router) Publish(e *Event) error {
	if e == nil {
		return errors.New("nil event")
	}
	// Augment event
	e.Timestamp = time.Now().UnixMilli()
	e.Seq = atomic.AddUint64(&r.nextSeq, 1)

	// persist first if configured (best-effort)
	if r.store != nil {
		_ = r.store.Append(e)
	}
	// enqueue with policy
	switch r.cfg.QueuePolicy {
	case QueueBlock:
		select {
		case r.queue <- e:
			return nil
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	case QueueDropOldest:
		select {
		case r.queue <- e:
			return nil
		default:
			// drop oldest
			select {
			case <-r.queue:
			default:
			}
			select {
			case r.queue <- e:
				return nil
			default:
				return errors.New("enqueue failed after dropping oldest")
			}
		}
	default: // QueueDropNewest
		select {
		case r.queue <- e:
			return nil
		default:
			// drop newest i.e. drop this event
			return errors.New("queue full, drop newest")
		}
	}
}

// AppendEvent alias Publish
func (r *Router) AppendEvent(e *Event) error {
	return r.Publish(e)
}

func (r *Router) dispatcher() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case e := <-r.queue:
			r.dispatchEvent(e)
		}
	}
}

func (r *Router) dispatchEvent(e *Event) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, s := range r.subs {
		// 先快速匹配，减少函数调用开销
		if s.filter != nil {
			if !s.filter(e) {
				continue
			}
		}
		// 非阻塞发送，若失败可记录 metric 或者采取替代策略
		ok := s.send(e)
		if !ok {
			// todo: 记录慢订阅者 metric / 日志
		}
	}
}

func uuidNew() string {
	// 轻量生成 id，避免引入额外依赖在此处直接使用 time+rand 或外部包
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func (e *Router) Process() {
	for {
		select {
		case event := <-e.queue:
			topic := toTopicStr(event.Address, event.Type)
			log.Infof("Receive task with topic %s", topic)
			e.startStopMu.RLock()

			publisher, ok := e.publishMap[topic]
			if publisher == nil || !ok {
				e.startStopMu.RUnlock()
				continue
			}

			eventData, err := json.Marshal(event)
			if err != nil {
				log.WithError(err).Debugln("Marshal event to bytes failed.")
				e.startStopMu.RUnlock()
				continue
			}

			publisher.Publish(eventData)

			e.startStopMu.RUnlock()
		}
	}
}

func (e *Router) HandleConnect(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.WithError(err).Errorln("Handle event websocket request failed.")
		conn.Close()
		return
	}

	_, message, err := conn.ReadMessage()
	log.Infof("Receive message from websocket: %s", message)

	request, err := DeserializeEventRequest(message)

	if err != nil {
		log.WithError(err).Errorln("Deserialize request to object failed.")
		conn.Close()
		return
	}

	if request.Address == "" || request.EventType == "" {
		log.Errorln("Address or type is empty...")
		conn.Close()
		return
	}

	topic := toTopicStr(request.Address, request.EventType)
	log.Infof("Receive subscribe with topic %s", topic)

	e.startStopMu.Lock()
	defer e.startStopMu.Unlock()
	publisher, ok := e.publishMap[topic]

	if !ok || publisher == nil {
		e.publishMap[topic] = CreateNewEventPublisher()
	}
	publisher, _ = e.publishMap[topic]

	if publisher.Full() {
		log.Debugln("Publisher connection is full.")
		conn.Close()
		return
	}

	publisher.AppendNewConnection(conn)
}

func toTopicStr(address string, eventType string) EventTopic {
	strTopic := fmt.Sprintf("%s#%s", address,
		eventType)

	return EventTopic(strTopic)
}
