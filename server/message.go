package server

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/yosisa/pluq/server/param"
	"github.com/yosisa/pluq/storage"
	"github.com/yosisa/pluq/uid"
	"golang.org/x/net/context"
)

func push(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	queue := param.FromContext(ctx, "queue")
	driver := storage.FromContext(ctx)
	id, err := uid.NextID(ctx)
	if err != nil {
		return err
	}
	msg, err := newMessage(r)
	if err != nil {
		return err
	}
	return driver.Enqueue(queue, id, msg)
}

func pop(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	queue := param.FromContext(ctx, "queue")
	driver := storage.FromContext(ctx)
	eid, err := uid.NextID(ctx)
	if err != nil {
		return err
	}
	msg, err := driver.Dequeue(queue, eid)
	if err != nil {
		return err
	}
	w.Header().Set("X-Pluq-Message-Id", eid.HashID())
	w.Header().Set("X-Pluq-Retry-Remaining", strconv.Itoa(msg.Retry))
	w.Header().Set("X-Pluq-Timeout", msg.Timeout.String())
	w.Header().Set("Content-Type", msg.ContentType)
	w.Write(msg.Body)
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

func newMessage(r *http.Request) (*storage.Message, error) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	msg := storage.NewMessage()
	msg.Body = b

	if s := r.URL.Query().Get("retry"); s != "" {
		if s == "nolimit" {
			msg.Retry = storage.RetryNoLimit
		} else {
			n, err := strconv.Atoi(s)
			if err != nil {
				return nil, err
			}
			msg.Retry = n
		}
	}
	msg.IncrRetry() // +1 for first attempt

	if s := r.URL.Query().Get("timeout"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, err
		}
		msg.Timeout = d
	}

	msg.ContentType = r.Header.Get("Content-Type")
	return msg, nil
}
