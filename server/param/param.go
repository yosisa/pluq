package param

import (
	"github.com/julienschmidt/httprouter"
	"golang.org/x/net/context"
)

type key int

const paramsKey key = iota

func NewContext(ctx context.Context, params httprouter.Params) context.Context {
	return context.WithValue(ctx, paramsKey, params)
}

func FromContext(ctx context.Context, name string) string {
	if params, ok := ctx.Value(paramsKey).(httprouter.Params); ok {
		return params.ByName(name)
	}
	return ""
}
