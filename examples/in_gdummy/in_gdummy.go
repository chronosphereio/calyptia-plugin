package main

import (
	"context"
	"errors"
	"time"

	"github.com/calyptia/plugin"
	"github.com/calyptia/cmetrics-go"
)

var counter_success *cmetrics.Counter
var counter_failure *cmetrics.Counter

func init() {
	plugin.RegisterInput("gdummy", "dummy GO!", gdummyPlugin{})
}

type gdummyPlugin struct{}

func (plug gdummyPlugin) Init(ctx context.Context, conf plugin.ConfigLoader, cmt *cmetrics.Context) error {
	var err error

	counter_success, err = cmt.CounterCreate("fluentbit", "input",
		"operation_succeeded_total", "Total number of succeeded operations", []string{"plugin_name"})
	if err != nil {
		return errors.New("Cannot create counter_success")
	}

	counter_failure, err = cmt.CounterCreate("fluentbit", "input",
		"operation_failed_total", "Total number of failed operations", []string{"plugin_name"})
	if err != nil {
		return errors.New("Cannot create counter_failure")
	}

	return nil
}

func (plug gdummyPlugin) Collect(ctx context.Context, ch chan<- plugin.Message) error {
	tick := time.NewTicker(time.Second)

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil && !errors.Is(err, context.Canceled) {
				return counter_failure.Inc(time.Now(), []string{"in_gdummy"})
			}

			return nil
		case <-tick.C:
			ch <- plugin.Message{
				Time: time.Now(),
				Record: map[string]string{
					"message": "dummy",
				},
			}
			err := counter_success.Inc(time.Now(), []string{"in_gdummy"})
			if err != nil {
				return err
			}
		}
	}
}

func main() {
}
