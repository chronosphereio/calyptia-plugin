package main

import (
	"context"
	"errors"
	"time"

	"github.com/calyptia/plugin"
	"github.com/calyptia/plugin/metric"
)

func init() {
	plugin.RegisterInput("gdummy", "dummy GO!", &gdummyPlugin{})
}

type gdummyPlugin struct {
	counterSuccess metric.Counter
	counterFailure metric.Counter
}

func (plug *gdummyPlugin) Init(ctx context.Context, conf plugin.ConfigLoader, metrics plugin.Metrics) error {
	plug.counterSuccess = metrics.NewCounter("operation_succeeded_total", "Total number of succeeded operations", "gdummy")
	plug.counterFailure = metrics.NewCounter("operation_failed_total", "Total number of failed operations", "gdummy")
	return nil
}

func (plug gdummyPlugin) Collect(ctx context.Context, ch chan<- plugin.Message) error {
	tick := time.NewTicker(time.Second)

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil && !errors.Is(err, context.Canceled) {
				plug.counterFailure.Add(1)

				return err
			}

			return nil
		case <-tick.C:
			plug.counterSuccess.Add(1)

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
