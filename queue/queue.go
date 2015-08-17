package queue

import (
	"strings"

	"github.com/yosisa/pluq/storage"
	"github.com/yosisa/pluq/uid"
)

type Manager struct {
	idg  *uid.Generator
	sd   storage.Driver
	root *node
}

func NewManager(idg *uid.Generator, sd storage.Driver) *Manager {
	return &Manager{
		idg:  idg,
		sd:   sd,
		root: newNode(),
	}
}

func (q *Manager) Enqueue(name string, msg *storage.Message, p *Properties) (map[string]*storage.EnqueueMeta, error) {
	out := make(map[string]*storage.EnqueueMeta)
	for _, v := range q.root.findQueue(split(name)) {
		key := v.name()
		v.props.merge(p)
		id, err := q.idg.Next()
		if err != nil {
			return out, err
		}
		var opts storage.EnqueueOptions
		if v.props.AccumTime != nil {
			opts.AccumTime = *v.props.AccumTime
		}
		meta, err := q.sd.Enqueue(key, id, newEnvelope(key, v.props, msg), &opts)
		if err != nil {
			return out, err
		}
		out[key] = meta
	}
	return out, nil
}

func (q *Manager) Dequeue(name string) (e *storage.Envelope, eid uid.ID, err error) {
	if eid, err = q.idg.Next(); err != nil {
		return
	}
	for _, v := range q.root.findQueue(split(name)) {
		key := v.name()
		if e, err = q.sd.Dequeue(key, eid); err == nil {
			e.Queue = key
			return
		}
		if err != storage.ErrEmpty {
			return
		}
	}
	err = storage.ErrEmpty
	return
}

func (q *Manager) Ack(eid uid.ID) error {
	return q.sd.Ack(eid)
}

func (q *Manager) Properties(name string, inherit bool) *Properties {
	keys := split(name)
	if inherit {
		return q.root.properties(keys)
	}
	if node := q.root.lookup(keys); node != nil {
		return node.props
	}
	return nil
}

func (q *Manager) SetProperties(name string, props *Properties) {
	q.root.setProperties(split(name), props)
}

func split(name string) []string {
	if name == "" {
		return nil
	}
	return strings.Split(name, "/")
}

func newEnvelope(queue string, props *Properties, msg *storage.Message) *storage.Envelope {
	e := storage.NewEnvelope()
	e.Queue = queue
	if props.Retry != nil {
		e.Retry = *props.Retry
	}
	e.Retry.Incr() // +1 for first attempt

	if props.Timeout != nil {
		e.Timeout = *props.Timeout
	}
	e.AddMessage(msg)
	return e
}
