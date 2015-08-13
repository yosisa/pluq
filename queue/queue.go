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

func (q *Manager) Enqueue(name string, msg *storage.Message, p *Properties) (*storage.EnqueueMeta, error) {
	id, err := q.idg.Next()
	if err != nil {
		return nil, err
	}
	props := q.Properties(name, true)
	props.merge(p)
	var opts storage.EnqueueOptions
	if props.AccumTime != nil {
		opts.AccumTime = *props.AccumTime
	}
	return q.sd.Enqueue(name, id, newEnvelope(props, msg), &opts)
}

func (q *Manager) Dequeue(name string) (e *storage.Envelope, eid uid.ID, err error) {
	if eid, err = q.idg.Next(); err != nil {
		return
	}
	e, err = q.sd.Dequeue(name, eid)
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
	return strings.Split(name, "/")
}

func newEnvelope(props *Properties, msg *storage.Message) *storage.Envelope {
	e := storage.NewEnvelope()
	if props.Retry != nil {
		e.Retry = *props.Retry
	}
	e.IncrRetry() // +1 for first attempt

	if props.Timeout != nil {
		e.Timeout = *props.Timeout
	}
	e.AddMessage(msg)
	return e
}
