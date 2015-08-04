package bolt

import "github.com/yosisa/pluq/storage"

//go:generate msgp
type Message struct {
	Body        []byte                 `msg:"body"`
	ContentType string                 `msg:"ct"`
	Meta        map[string]interface{} `msg:"meta"`
}

func marshal(src *storage.Message) ([]byte, error) {
	m := &Message{
		Body:        src.Body,
		ContentType: src.ContentType,
		Meta:        src.Meta,
	}
	return m.MarshalMsg(nil)
}

func unmarshal(b []byte) (*Message, error) {
	var m Message
	_, err := m.UnmarshalMsg(b)
	return &m, err
}
