package main

import (
	"context"
	"fmt"
	"time"

	"github.com/fluent/fluent-bit-go/plugin"
)

func init() {
	plugin.RegisterOutput("go-dummy-output-plugin", "Dummy golang output plugin for testing", &dummyPlugin{})
}

type dummyPlugin struct {
	foo string
}

func (plug *dummyPlugin) Setup(ctx context.Context, conf plugin.ConfigLoader) error {
	plug.foo = conf.Load("foo")
	return nil
}

func (plug *dummyPlugin) Run(ctx context.Context, tag string, ch <-chan plugin.Message) error {
	for msg := range ch {
		fmt.Printf("tag=%s time=%s record=%+v\n", tag, msg.Time.Format(time.RFC3339), msg.Record)
	}

	return nil
}

func main() {}
