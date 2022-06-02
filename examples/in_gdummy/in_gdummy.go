package main

import (
	"context"
	"errors"
	"time"

	"github.com/calyptia/plugin"
)

func init() {
	plugin.RegisterInput("gdummy", "dummy GO!", gdummyPlugin{})
}

type gdummyPlugin struct{}

func (plug gdummyPlugin) Init(ctx context.Context, conf plugin.ConfigLoader) error {
	return nil
}

func (plug gdummyPlugin) Collect(ctx context.Context, ch chan<- plugin.Message) error {
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
					"message": "dummy",
				},
			}
		}
	}
}

func main() {
}
