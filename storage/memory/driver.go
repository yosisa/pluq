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
	availAt int64
	queue   string
	m       *storage.Message
	eid     uid.ID
	removed bool
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

type Driver struct {
	schedule       messageHeap
	ephemeralIndex map[uid.ID]*message
	m              sync.Mutex
}

func New() *Driver {
	d := &Driver{
		ephemeralIndex: make(map[uid.ID]*message),
	}
	heap.Init(&d.schedule)
	return d
}

func (d *Driver) Enqueue(queue string, id uid.ID, m *storage.Message) error {
	msg := &message{
		availAt: time.Now().UnixNano(),
		queue:   queue,
		m:       m,
	}
	d.m.Lock()
	defer d.m.Unlock()
	heap.Push(&d.schedule, msg)
	return nil
}

func (d *Driver) Dequeue(queue string, eid uid.ID) (m *storage.Message, err error) {
	now := time.Now().UnixNano()
	d.m.Lock()
	defer d.m.Unlock()
	for i, n := 0, len(d.schedule); i < n; i++ {
		msg := d.schedule[i]
		if msg.availAt > now {
			break
		}
		if !msg.m.CanRetry() {
			event.Emit(event.EventMessageDiscarded, msg.m)
			msg.removed = true
		}
		if msg.removed {
			heap.Remove(&d.schedule, i)
			i--
			n--
			continue
		}
		if msg.queue == queue {
			m = msg.m
			msg.eid = eid
			msg.availAt = now + int64(msg.m.Timeout)
			msg.m.DecrRetry()
			heap.Fix(&d.schedule, i)
			d.ephemeralIndex[eid] = msg
			return
		}
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

func (d *Driver) Close() error {
	return nil
}
