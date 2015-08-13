package queue

import "golang.org/x/net/context"

type key int

const managerKey key = iota

func NewContext(ctx context.Context, q *Manager) context.Context {
	return context.WithValue(ctx, managerKey, q)
}

func FromContext(ctx context.Context) *Manager {
	if q, ok := ctx.Value(managerKey).(*Manager); ok {
		return q
	}
	return nil
}
