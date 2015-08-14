package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"

	"github.com/yosisa/pluq/queue"
	"github.com/yosisa/pluq/server/param"
	"github.com/yosisa/pluq/storage"
	"github.com/yosisa/pluq/types"
	"github.com/yosisa/pluq/uid"
	"golang.org/x/net/context"
)

func push(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	name := queueName(ctx)
	q := queue.FromContext(ctx)
	props, err := newProperties(r)
	if err != nil {
		return err
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	msg := &storage.Message{
		ContentType: r.Header.Get("Content-Type"),
		Body:        b,
	}
	meta, err := q.Enqueue(name, msg, props)
	if err != nil {
		return err
	}
	results := make(map[string]*pushResult)
	for k, v := range meta {
		results[k] = newPushResult(v)
	}
	return json.NewEncoder(w).Encode(results)
}

type pushResult struct {
	AccumState string `json:"accum_state"`
}

func newPushResult(meta *storage.EnqueueMeta) *pushResult {
	var r pushResult
	switch meta.AccumState {
	case storage.AccumStarted:
		r.AccumState = "started"
	case storage.AccumAdded:
		r.AccumState = "added"
	default:
		r.AccumState = "disabled"
	}
	return &r
}

func pop(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	name := queueName(ctx)
	q := queue.FromContext(ctx)
	envelope, eid, err := q.Dequeue(name)
	if err != nil {
		return err
	}
	return writeHTTP(w, eid, envelope)
}

func reply(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	eid, err := uid.FromHashID(param.FromContext(ctx, "id"))
	if err != nil {
		return err
	}
	q := queue.FromContext(ctx)
	if err := q.Ack(eid); err != nil {
		return err
	}
	fmt.Fprintf(w, "ok")
	return nil
}

func newProperties(r *http.Request) (*queue.Properties, error) {
	props := queue.NewProperties()
	if s := r.URL.Query().Get("retry"); s != "" {
		n, err := types.ParseRetry(s)
		if err != nil {
			return nil, err
		}
		props.SetRetry(n)
	}

	if s := r.URL.Query().Get("timeout"); s != "" {
		d, err := types.ParseDuration(s)
		if err != nil {
			return nil, err
		}
		props.SetTimeout(d)
	}

	if s := r.URL.Query().Get("accum_time"); s != "" {
		d, err := types.ParseDuration(s)
		if err != nil {
			return nil, err
		}
		props.SetAccumTime(d)
	}

	return props, nil
}

func writeHTTP(w http.ResponseWriter, eid uid.ID, e *storage.Envelope) error {
	w.Header().Set("X-Pluq-Message-Id", eid.HashID())
	w.Header().Set("X-Pluq-Retry-Remaining", e.Retry.String())
	w.Header().Set("X-Pluq-Timeout", e.Timeout.String())
	if !e.IsComposite() {
		w.Header().Set("Content-Type", e.Messages[0].ContentType)
		w.Write(e.Messages[0].Body)
		return nil
	}
	mw := multipart.NewWriter(w)
	defer mw.Close()
	for _, msg := range e.Messages {
		mh := make(textproto.MIMEHeader)
		mh.Set("Content-Type", msg.ContentType)
		pw, err := mw.CreatePart(mh)
		if err != nil {
			return err
		}
		pw.Write(msg.Body)
	}
	return nil
}
