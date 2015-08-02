package server

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/yosisa/pluq/server/param"
	"github.com/yosisa/pluq/storage"
	"github.com/yosisa/pluq/uid"
	"golang.org/x/net/context"
)

func push(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	queue := param.FromContext(ctx, "queue")
	driver := storage.FromContext(ctx)
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	id, err := uid.NextID(ctx)
	if err != nil {
		return err
	}
	return driver.Enqueue(queue, id, b)
}

func pop(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	queue := param.FromContext(ctx, "queue")
	driver := storage.FromContext(ctx)
	eid, err := uid.NextID(ctx)
	if err != nil {
		return err
	}
	b, err := driver.Dequeue(queue, eid)
	if err != nil {
		return err
	}
	w.Header().Set("X-Pluq-Message-Id", eid.HashID())
	w.Write(b)
	return nil
}

func reply(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	eid, err := uid.FromHashID(param.FromContext(ctx, "id"))
	if err != nil {
		return err
	}
	driver := storage.FromContext(ctx)
	if err := driver.Ack(eid); err != nil {
		return err
	}
	fmt.Fprintf(w, "ok")
	return nil
}
