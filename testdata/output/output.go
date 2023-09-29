package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/calyptia/plugin"
	"github.com/calyptia/plugin/metric"
)

// TODO: investigate error:
// `fatal: morestack on g0`

func init() {
	plugin.RegisterOutput("go-test-output-plugin", "Golang output plugin for testing", &outputPlugin{})
}

type outputPlugin struct {
	filepath     string
	flushCounter metric.Counter
	log          plugin.Logger
}

func (plug *outputPlugin) Init(ctx context.Context, fbit *plugin.Fluentbit) error {
	plug.filepath = fbit.Conf.String("filepath")
	plug.flushCounter = fbit.Metrics.NewCounter("flush_total", "Total number of flushes", "go-test-output-plugin")
	plug.log = fbit.Logger
	return nil
}

func (plug outputPlugin) Flush(ctx context.Context, ch <-chan plugin.Message) error {
	f, err := os.Create(plug.filepath)
	if err != nil {
		plug.log.Error("[go-test-output-plugin] operation failed. reason %w", err)
		return fmt.Errorf("could not open output.txt: %w", err)
	}

	defer f.Close()

	for msg := range ch {
		if skip, ok := msg.Record["skipMe"].(bool); ok && skip {
			continue
		}

		err := json.NewEncoder(f).Encode(msg.Record)
		if err != nil {
			plug.log.Error("[go-test-output-plugin] operation failed. reason %w", err)
			return fmt.Errorf("could not write to output.txt: %w", err)
		}

		plug.log.Info("[go-test-output-plugin] operation succeeded")
		plug.flushCounter.Add(1)
	}

	return nil
}

func main() {}
