package queue

import (
	"strings"
	"time"

	"github.com/yosisa/pluq/event"
	"github.com/yosisa/pluq/storage"
	"github.com/yosisa/pluq/uid"
)

type meDriver struct {
	storage.Driver
}

func (d *meDriver) EnqueueAll(es []*storage.Envelope, eos []*storage.EnqueueOptions) (map[string]*storage.EnqueueMeta, error) {
	out := make(map[string]*storage.EnqueueMeta)
	for i, e := range es {
		meta, err := d.Enqueue(e.Queue, e.ID, e, eos[i])
		if err != nil {
			return out, err
		}
		out[e.Queue] = meta
	}
	return out, nil
}

type mdDriver struct {
	storage.Driver
}

func (d *mdDriver) DequeueAny(names []string, eid uid.ID) (e *storage.Envelope, err error) {
	for _, name := range names {
		if e, err = d.Dequeue(name, eid); err == nil {
			e.Queue = name
			return
		}
		if err != storage.ErrEmpty {
			return
		}
	}
	return nil, storage.ErrEmpty
}

type Manager struct {
	idg   *uid.Generator
	sd    storage.Driver
	sme   storage.MultiEnqueuer
	smd   storage.MultiDequeuer
	root  *node
	waits *waiters
}

func NewManager(idg *uid.Generator, sd storage.Driver) *Manager {
	m := &Manager{
		idg:   idg,
		sd:    sd,
		root:  newNode(),
		waits: &waiters{},
	}
	if sme, ok := sd.(storage.MultiEnqueuer); ok {
		m.sme = sme
	} else {
		m.sme = &meDriver{sd}
	}
	if smd, ok := sd.(storage.MultiDequeuer); ok {
		m.smd = smd
	} else {
		m.smd = &mdDriver{sd}
	}
	event.Handle(event.EventMessageAvailable, m)
	return m
}

func (q *Manager) Enqueue(name string, msg *storage.Message, p *Properties) (map[string]*storage.EnqueueMeta, error) {
	queues := q.root.findQueue(split(name))
	if len(queues) == 1 {
		e, es, err := q.prepareEnqueue(queues[0], msg, p)
		if err != nil {
			return nil, err
		}
		meta, err := q.sd.Enqueue(e.Queue, e.ID, e, es)
		if err != nil {
			return nil, err
		}
		return map[string]*storage.EnqueueMeta{e.Queue: meta}, nil
	}

	var es []*storage.Envelope
	var eos []*storage.EnqueueOptions
	for _, v := range queues {
		e, eo, err := q.prepareEnqueue(v, msg, p)
		if err != nil {
			return nil, err
		}
		es = append(es, e)
		eos = append(eos, eo)
	}
	return q.sme.EnqueueAll(es, eos)
}

func (q *Manager) prepareEnqueue(v *queue, msg *storage.Message, p *Properties) (*storage.Envelope, *storage.EnqueueOptions, error) {
	id, err := q.idg.Next()
	if err != nil {
		return nil, nil, err
	}
	v.props.merge(p)
	var opts storage.EnqueueOptions
	if v.props.AccumTime != nil {
		opts.AccumTime = *v.props.AccumTime
	}
	name := v.name()
	e := newEnvelope(name, v.props, msg)
	e.ID = id
	e.Queue = name
	return e, &opts, nil
}

func (q *Manager) Dequeue(name string, wait time.Duration) (e *storage.Envelope, err error) {
	var eid uid.ID
	if eid, err = q.idg.Next(); err != nil {
		return
	}
	queues := q.root.findQueue(split(name))
	if len(queues) == 1 {
		e, err = q.sd.Dequeue(queues[0].name(), eid)
	} else {
		var names []string
		for _, v := range queues {
			names = append(names, v.name())
		}
		e, err = q.smd.DequeueAny(names, eid)
	}
	if err != storage.ErrEmpty || wait == 0 {
		setEID(e, eid)
		return
	}

	// wait for a new message to be available
	err = nil
	w := newWaitRequest(q.root, name, wait)
	q.waits.add(w)
	select {
	case e = <-w.c:
	case <-time.After(wait):
		err = storage.ErrEmpty
	}
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

func (q *Manager) HandleEvent(e event.EventType, v interface{}) {
	name := v.(string)
	w := q.waits.find(name)
	if w == nil {
		return
	}
	eid, err := q.idg.Next()
	if err != nil {
		return
	}
	if e, err := q.sd.Dequeue(name, eid); err == nil {
		setEID(e, eid)
		w.handle(e)
		q.waits.remove(w)
	} else {
		q.waits.reset(w)
	}
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

func setEID(e *storage.Envelope, id uid.ID) {
	if e != nil {
		e.ID = id
	}
}
