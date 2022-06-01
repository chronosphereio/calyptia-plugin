package main

import (
	"context"
	"errors"
	"time"

	"github.com/calyptia/plugin"
	cmetrics "github.com/calyptia/cmetrics-go"
)

func init() {
	plugin.RegisterInput("go-test-input-plugin", "Golang input plugin for testing", &dummyPlugin{})
}

type dummyPlugin struct {
	foo string
}

func (plug *dummyPlugin) Init(ctx context.Context, conf plugin.ConfigLoader, cmt *cmetrics.Context) error {
	plug.foo = conf.String("foo")
	return nil
}

func (plug dummyPlugin) Collect(ctx context.Context, ch chan<- plugin.Message) error {
	tick := time.NewTicker(time.Second)

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}

			return nil
		case <-tick.C:
			ch <- plugin.Message{
				Time: time.Now(),
				Record: map[string]string{
					"message": "hello from go-test-input-plugin",
					"foo":     plug.foo,
				},
			}
		}
	}
}

func main() {}
