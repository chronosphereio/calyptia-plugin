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
	log            plugin.Logger
}

func (plug *gdummyPlugin) Init(ctx context.Context, fbit *plugin.Fluentbit) error {
	plug.counterSuccess = fbit.Metrics.NewCounter("operation_succeeded_total", "Total number of succeeded operations", "gdummy")
	plug.counterFailure = fbit.Metrics.NewCounter("operation_failed_total", "Total number of failed operations", "gdummy")
	plug.log = fbit.Logger

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
				plug.log.Error("[gdummy] operation failed")

				return err
			}

			return nil
		case <-tick.C:
			plug.counterSuccess.Add(1)
			plug.log.Debug("[gdummy] operation succeeded")

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
