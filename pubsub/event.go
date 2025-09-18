package pubsub

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

/*
  扩展字段：
	ID: 唯一事件 id（UUID 或 hex）
	Seq: Router 分发序列号（Router 内部生成）
	Timestamp: unix ms
	Namespace: 事件分类（例如 core/data、tx/pool 等）
	Type: 事件类型（必需）
	Hash, Height, Address: 业务字段（保持兼容）
	Params: map[string]string（可选）
	Source: 来源（local, p2p:peerid, module）
	Raw: 可选原始 bytes（用于高效序列化/反序列化）

*/

type Event struct {
	ID        string            `json:"id"`                  // 唯一 id
	Seq       uint64            `json:"seq,omitempty"`       // Router 分配的序号
	Namespace string            `json:"namespace,omitempty"` // 事件命名空间/源模块
	Type      string            `json:"type"`                // 事件类型
	Hash      string            `json:"hash,omitempty"`
	Height    int64             `json:"height,omitempty"`
	Address   string            `json:"address,omitempty"`
	Params    map[string]string `json:"params,omitempty"`
	Source    string            `json:"source,omitempty"`    // local / p2p:peerid / module
	Timestamp int64             `json:"timestamp,omitempty"` // unix ms
	Raw       []byte            `json:"raw,omitempty"`       // 可选原始 bytes
}

// NewEvent 创建一个基础事件，若 id 为空则生成
func NewEvent(typ string, params map[string]string) *Event {
	id := uuid.New().String()
	return &Event{
		ID:        id,
		Type:      typ,
		Params:    params,
		Timestamp: time.Now().UnixMilli(),
	}
}

func (e *Event) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

func EventFromJSON(b []byte) (*Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	return &e, nil
}
