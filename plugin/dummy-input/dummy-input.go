package main

import (
	"context"
	"time"

	"github.com/fluent/fluent-bit-go/plugin"
)

func init() {
	plugin.RegisterInput("go-dummy-input-plugin", "Dummy golang input plugin for testing", &dummyPlugin{})
}

type dummyPlugin struct {
	foo string
}

func (plug *dummyPlugin) Setup(ctx context.Context, conf plugin.ConfigLoader) error {
	plug.foo = conf.Load("foo")
	return nil
}

func (plug *dummyPlugin) Run(ctx context.Context, ch chan<- plugin.Message) error {
	for i := 0; i < 10; i++ {
		ch <- plugin.Message{
			Time: time.Now(),
			Record: map[string]string{
				"message": "hello from go-dummy-plugin",
				"foo":     plug.foo,
			},
		}

		time.Sleep(time.Second)
	}

	return nil
}

func main() {}
