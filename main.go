package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/jawher/mow.cli"
	"github.com/yosisa/pluq/event"
	"github.com/yosisa/pluq/queue"
	"github.com/yosisa/pluq/server"
	"github.com/yosisa/pluq/storage"
	"github.com/yosisa/pluq/storage/bolt"
	"github.com/yosisa/pluq/storage/memory"
	"github.com/yosisa/pluq/uid"
	"golang.org/x/net/context"
)

func newStorageDriver(s string) (storage.Driver, error) {
	switch s {
	case "memory":
		return memory.New(), nil
	case "bolt":
		return bolt.New("pluq.db")
	}
	return nil, fmt.Errorf("Unsupported storage driver: %s", s)
}

func main() {
	event.HandleAll(event.HandlerFunc(func(e event.EventType, v interface{}) {
		log.Printf("Event #%d: %+v", e, v)
	}))
	go event.Dispatch()

	app := cli.App("pluq", "A pluggable message queue")
	storageDriver := app.StringOpt("storage-driver", "bolt", "Change the storage driver (bolt|memory)")
	listen := app.StringOpt("l listen", ":3900", "Listen address")
	app.Action = func() {
		idgen, err := uid.NewGenerator(0)
		if err != nil {
			log.Fatal(err)
		}

		d, err := newStorageDriver(*storageDriver)
		if err != nil {
			log.Fatal(err)
		}
		defer d.Close()

		ctx := context.Background()
		ctx = queue.NewContext(ctx, queue.NewManager(idgen, d))

		if err := http.ListenAndServe(*listen, server.New(ctx)); err != nil {
			log.Fatal(err)
		}
	}
	app.Run(os.Args)
}
