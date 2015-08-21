package memory

import (
	"container/heap"
	"sync"
	"time"

	"github.com/yosisa/pluq/event"
	"github.com/yosisa/pluq/storage"
	"github.com/yosisa/pluq/uid"
)

type message struct {
	availAt     int64
	envelope    *storage.Envelope
	eid         uid.ID
	removed     bool
	accumlating bool
}

type messageHeap []*message

func (h messageHeap) Len() int {
	return len(h)
}

func (h messageHeap) Less(i, j int) bool {
	return h[i].availAt < h[j].availAt
}

func (h messageHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *messageHeap) Push(x interface{}) {
	*h = append(*h, x.(*message))
}

func (h *messageHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type queueIndex struct {
	index map[string]*messageHeap
	m     sync.Mutex
}

func newQueueIndex() *queueIndex {
	return &queueIndex{
		index: make(map[string]*messageHeap),
	}
}

func (q *queueIndex) get(name string) *messageHeap {
	if h, ok := q.index[name]; ok {
		return h
	}
	q.m.Lock()
	defer q.m.Unlock()
	if h, ok := q.index[name]; ok {
		return h
	}
	var mh messageHeap
	q.index[name] = &mh
	return &mh
}

type Driver struct {
	queues         *queueIndex
	ephemeralIndex map[uid.ID]*message
	m              sync.Mutex
}

func New() *Driver {
	d := &Driver{
		queues:         newQueueIndex(),
		ephemeralIndex: make(map[uid.ID]*message),
	}
	return d
}

func (d *Driver) Enqueue(queue string, id uid.ID, e *storage.Envelope, opts *storage.EnqueueOptions) (*storage.EnqueueMeta, error) {
	var meta storage.EnqueueMeta
	msgs := d.queues.get(queue)
	now := time.Now().UnixNano()

	d.m.Lock()
	defer d.m.Unlock()
	if opts.AccumTime > 0 {
		for _, msg := range *msgs {
			if msg.availAt > now && msg.accumlating {
				meta.AccumState = storage.AccumAdded
				msg.envelope.AddMessage(e.Messages[0])
				return &meta, nil
			}
		}
	}

	msg := &message{
		availAt:  now,
		envelope: e,
	}
	if opts.AccumTime > 0 {
		msg.availAt += int64(opts.AccumTime)
		msg.accumlating = true
		meta.AccumState = storage.AccumStarted
	}
	heap.Push(msgs, msg)
	if opts.AccumTime == 0 {
		event.Emit(event.EventMessageAvailable, queue)
	}
	return &meta, nil
}

func (d *Driver) Dequeue(queue string, eid uid.ID) (e *storage.Envelope, err error) {
	now := time.Now().UnixNano()
	d.m.Lock()
	defer d.m.Unlock()
	msgs := d.queues.get(queue)
	for i, n := 0, len(*msgs); i < n; i++ {
		msg := (*msgs)[i]
		if msg.availAt > now {
			break
		}
		if !msg.envelope.Retry.IsValid() {
			event.Emit(event.EventMessageDiscarded, msg.envelope)
			msg.removed = true
		}
		if msg.removed {
			heap.Remove(msgs, i)
			i--
			n--
			continue
		}
		e = msg.envelope
		msg.eid = eid
		msg.availAt = now + int64(msg.envelope.Timeout)
		msg.envelope.Retry.Decr()
		msg.accumlating = false
		heap.Fix(msgs, i)
		d.ephemeralIndex[eid] = msg
		return
	}
	err = storage.ErrEmpty
	return
}

func (d *Driver) Ack(eid uid.ID) error {
	now := time.Now().UnixNano()
	d.m.Lock()
	defer d.m.Unlock()
	msg := d.ephemeralIndex[eid]
	if msg == nil || msg.availAt <= now || msg.eid != eid || msg.removed {
		return storage.ErrInvalidEphemeralID
	}
	msg.removed = true // Actual removing is performed in dequeue
	return nil
}

func (d *Driver) Reset(eid uid.ID) error {
	now := time.Now().UnixNano()
	d.m.Lock()
	defer d.m.Unlock()
	msg := d.ephemeralIndex[eid]
	if msg == nil || msg.availAt <= now || msg.eid != eid || msg.removed {
		return storage.ErrInvalidEphemeralID
	}
	msgs := d.queues.get(msg.envelope.Queue)
	for i, m := range *msgs {
		if m == msg {
			m.availAt = 0
			m.envelope.Retry.Incr()
			heap.Fix(msgs, i)
			return nil
		}
	}
	return storage.ErrInvalidEphemeralID
}

func (d *Driver) Close() error {
	return nil
}
