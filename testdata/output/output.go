package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/calyptia/plugin"
	"github.com/calyptia/plugin/metric"
)

func init() {
	plugin.RegisterOutput("go-test-output-plugin", "Golang output plugin for testing", &outputPlugin{})
}

type outputPlugin struct {
	flushCounter metric.Counter
	log          plugin.Logger
}

func (plug *outputPlugin) Init(ctx context.Context, fbit *plugin.Fluentbit) error {
	plug.flushCounter = fbit.Metrics.NewCounter("flush_total", "Total number of flushes", "go-test-output-plugin")
	plug.log = fbit.Logger
	return nil
}

func (plug outputPlugin) Flush(ctx context.Context, ch <-chan plugin.Message) error {
	f, err := os.Create("/fluent-bit/etc/output.txt")
	if err != nil {
		plug.log.Error("[go-test-output-plugin] operation failed. reason %w", err)
		return fmt.Errorf("could not open output.txt: %w", err)
	}

	defer f.Close()

	for msg := range ch {
		plug.flushCounter.Add(1)
		plug.log.Info("[go-test-output-plugin] operation proceeded")

		m, ok := msg.Record.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected record type: %T", msg.Record)
		}

		foo, ok := m["foo"].(string)
		if !ok {
			return fmt.Errorf("unexpected record type: %T", m["foo"])
		}

		message := m["message"].(string)
		if !ok {
			return fmt.Errorf("unexpected record type: %T", m["message"])
		}

		tmpl := m["template"].(string)
		if !ok {
			return fmt.Errorf("unexpected record type: %T", m["template"])
		}

		_, err := fmt.Fprintf(f, "message=\"got record\" tag=%s time=%s record_foo=%s record_message=%q record_tmpl=%q\n", msg.Tag(), msg.Time.Format(time.RFC3339), foo, message, tmpl)
		if err != nil {
			plug.log.Error("[go-test-output-plugin] operation failed. reason %w", err)
			return fmt.Errorf("could not write to output.txt: %w", err)
		}
	}

	return nil
}

func main() {}
