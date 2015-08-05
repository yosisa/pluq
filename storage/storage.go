package storage

import (
	"errors"
	"time"

	"github.com/yosisa/pluq/uid"
	"golang.org/x/net/context"
)

var (
	ErrEmpty              = errors.New("Error empty queue")
	ErrInvalidEphemeralID = errors.New("Error invalid ephemeral id")
)

type Driver interface {
	Enqueue(string, uid.ID, *Envelope, *EnqueueOptions) (*EnqueueMeta, error)
	Dequeue(string, uid.ID) (*Envelope, error)
	Ack(uid.ID) error
	Close() error
}

type EnqueueOptions struct {
	AccumTime time.Duration
}

type AccumState int

const (
	AccumDisabled AccumState = iota
	AccumStarted
	AccumAdded
)

type EnqueueMeta struct {
	AccumState AccumState
}

type key int

const driverKey = iota

func NewContext(ctx context.Context, d Driver) context.Context {
	return context.WithValue(ctx, driverKey, d)
}

func FromContext(ctx context.Context) Driver {
	if d, ok := ctx.Value(driverKey).(Driver); ok {
		return d
	}
	return nil
}
