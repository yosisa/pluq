package uid

import "golang.org/x/net/context"

type key int

const idGenKey key = iota

func NewContext(ctx context.Context, g *Generator) context.Context {
	return context.WithValue(ctx, idGenKey, g)
}

func NextID(ctx context.Context) (ID, error) {
	if g, ok := ctx.Value(idGenKey).(*Generator); ok {
		return g.Next()
	}
	panic("No ID Generator")
}
