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

var RetryWait = 30 * time.Second

type Driver interface {
	Enqueue(string, uid.ID, []byte) error
	Dequeue(string, uid.ID) ([]byte, error)
	Ack(uid.ID) error
	Close() error
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
