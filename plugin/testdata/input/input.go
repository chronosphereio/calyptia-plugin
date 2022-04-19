package main

import (
	"context"
	"time"

	"github.com/fluent/fluent-bit-go/plugin"
)

func init() {
	plugin.RegisterInput("go-test-input-plugin", "Golang input plugin for testing", &dummyPlugin{})
}

type dummyPlugin struct {
	foo string
}

func (plug *dummyPlugin) Init(ctx context.Context, conf plugin.ConfigLoader) error {
	plug.foo = conf.String("foo")
	return nil
}

func (plug dummyPlugin) Collect(ctx context.Context, ch chan<- plugin.Message) error {
	for {
		ch <- plugin.Message{
			Time: time.Now(),
			Record: map[string]string{
				"message": "hello from go-test-input-plugin",
				"foo":     plug.foo,
			},
		}

		time.Sleep(time.Second)
	}
}

func main() {}
