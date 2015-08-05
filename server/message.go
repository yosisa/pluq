package server

import (
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
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
	msg, err := newEnvelope(r)
	if err != nil {
		return err
	}
	var opts storage.EnqueueOptions
	if s := r.URL.Query().Get("accum_time"); s != "" {
		if opts.AccumTime, err = time.ParseDuration(s); err != nil {
			return err
		}
	}
	meta, err := driver.Enqueue(queue, id, msg, &opts)
	if err != nil {
		return err
	}
	accum := "disabled"
	switch meta.AccumState {
	case storage.AccumStarted:
		accum = "started"
	case storage.AccumAdded:
		accum = "added"
	}
	fmt.Fprintf(w, `{"accum_state":"%s"}`, accum)
	return nil
}

func pop(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	queue := param.FromContext(ctx, "queue")
	driver := storage.FromContext(ctx)
	eid, err := uid.NextID(ctx)
	if err != nil {
		return err
	}
	envelope, err := driver.Dequeue(queue, eid)
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
	driver := storage.FromContext(ctx)
	if err := driver.Ack(eid); err != nil {
		return err
	}
	fmt.Fprintf(w, "ok")
	return nil
}

func newEnvelope(r *http.Request) (*storage.Envelope, error) {
	envelope := storage.NewEnvelope()
	if s := r.URL.Query().Get("retry"); s != "" {
		if s == "nolimit" {
			envelope.Retry = storage.RetryNoLimit
		} else {
			n, err := strconv.Atoi(s)
			if err != nil {
				return nil, err
			}
			envelope.Retry = n
		}
	}
	envelope.IncrRetry() // +1 for first attempt

	if s := r.URL.Query().Get("timeout"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, err
		}
		envelope.Timeout = d
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	envelope.AddMessage(&storage.Message{
		ContentType: r.Header.Get("Content-Type"),
		Body:        b,
	})
	return envelope, nil
}

func writeHTTP(w http.ResponseWriter, eid uid.ID, e *storage.Envelope) error {
	w.Header().Set("X-Pluq-Message-Id", eid.HashID())
	w.Header().Set("X-Pluq-Retry-Remaining", strconv.Itoa(e.Retry))
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
