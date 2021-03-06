package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/yosisa/pluq/queue"
	"golang.org/x/net/context"
)

func setProperties(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	name := queueName(ctx)
	q := queue.FromContext(ctx)
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	props := queue.NewProperties()
	if err := json.Unmarshal(b, props); err != nil {
		return err
	}
	q.SetProperties(name, props)
	return nil
}

func getProperties(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	name := queueName(ctx)
	q := queue.FromContext(ctx)
	inherit := asBool(r.URL.Query().Get("inherit"))
	if props := q.Properties(name, inherit); props != nil {
		return json.NewEncoder(w).Encode(props)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
