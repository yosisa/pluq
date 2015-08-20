package queue

import (
	"errors"
	"sync"
	"time"

	"github.com/yosisa/pluq/storage"
)

var errTimeout = errors.New("wait request timed out")

type waiter interface {
	match(string) (bool, error)
	handle(*storage.Envelope)
}

type waitItem struct {
	w        waiter
	borrowed bool
}

type waiters struct {
	waits []*waitItem
	m     sync.RWMutex
}

func (d *waiters) add(w waiter) {
	d.m.Lock()
	defer d.m.Unlock()
	d.waits = append(d.waits, &waitItem{w: w})
}

func (d *waiters) remove(w waiter) {
START:
	d.m.RLock()
	for i, wi := range d.waits {
		if wi.w == w {
			d.m.RUnlock()
			d.m.Lock()
			if len(d.waits) <= i || d.waits[i] != wi {
				d.m.Unlock()
				goto START
			}
			defer d.m.Unlock()
			copy(d.waits[i:], d.waits[i+1:])
			d.waits = d.waits[:len(d.waits)-1]
			return
		}
	}
	d.m.RUnlock()
}

func (d *waiters) find(name string) waiter {
START:
	d.m.RLock()
	for i, w := range d.waits {
		if w.borrowed {
			continue
		}
		if ok, err := w.w.match(name); ok || err != nil {
			d.m.RUnlock()
			d.m.Lock()
			if len(d.waits) <= i || d.waits[i] != w || w.borrowed {
				// Changed in other goroutines
				d.m.Unlock()
				goto START
			}

			if err != nil {
				copy(d.waits[i:], d.waits[i+1:])
				d.waits = d.waits[:len(d.waits)-1]
				d.m.Unlock()
				goto START
			}

			defer d.m.Unlock()
			w.borrowed = true
			return w.w
		}
	}
	d.m.RUnlock()
	return nil
}

func (d *waiters) reset(w waiter) {
START:
	d.m.RLock()
	for i, wi := range d.waits {
		if wi.w == w {
			d.m.RUnlock()
			d.m.Lock()
			if len(d.waits) <= i || d.waits[i] != wi {
				d.m.Unlock()
				goto START
			}
			defer d.m.Unlock()
			wi.borrowed = false
			return
		}
	}
	d.m.RUnlock()
}

type waitRequest struct {
	root     *node
	keys     []string
	c        chan *storage.Envelope
	deadline time.Time
}

func newWaitRequest(root *node, name string, wait time.Duration) *waitRequest {
	return &waitRequest{
		root:     root,
		keys:     split(name),
		c:        make(chan *storage.Envelope),
		deadline: time.Now().Add(wait),
	}
}

func (w *waitRequest) match(name string) (bool, error) {
	if time.Now().After(w.deadline) {
		return false, errTimeout
	}
	for _, v := range w.root.findQueue(w.keys) {
		if v.name() == name {
			return true, nil
		}
	}
	return false, nil
}

func (w *waitRequest) handle(e *storage.Envelope) {
	w.c <- e
	close(w.c)
}
