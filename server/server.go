package server

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/yosisa/pluq/server/param"
	"github.com/yosisa/pluq/storage"
	"golang.org/x/net/context"
)

type Handle func(context.Context, http.ResponseWriter, *http.Request) error

type Middleware func(Handle) Handle

func New(ctx context.Context) http.Handler {
	f := apiFactory(ctx)
	router := httprouter.New()
	router.POST("/v1/queues/:queue/messages", f(push))
	router.GET("/v1/queues/:queue/messages", f(pop))
	router.DELETE("/v1/queues/:queue/messages/:id", f(reply))
	return router
}

func apiFactory(rootCtx context.Context) func(h Handle, ms ...Middleware) httprouter.Handle {
	if rootCtx == nil {
		rootCtx = context.Background()
	}
	return func(h Handle, ms ...Middleware) httprouter.Handle {
		for i := len(ms) - 1; i >= 0; i-- {
			h = ms[i](h)
		}
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			ctx := param.NewContext(rootCtx, ps)
			if err := h(ctx, w, r); err != nil {
				handleError(ctx, w, r, err)
			}
		}
	}
}

func handleError(ctx context.Context, w http.ResponseWriter, r *http.Request, err error) {
	switch err {
	case storage.ErrEmpty:
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err)
	}
}
