package main

import (
	"context"
	"fmt"
	"reflect"

	"github.com/calyptia/plugin"
	"github.com/calyptia/plugin/metric"
)

func init() {
	plugin.RegisterOutput("gstdout", "StdOut GO!", &gstdoutPlugin{})
}

type gstdoutPlugin struct {
	param        string
	flushCounter metric.Counter
	log          plugin.Logger
}

func (plug *gstdoutPlugin) Init(ctx context.Context, fbit *plugin.Fluentbit) error {
	plug.flushCounter = fbit.Metrics.NewCounter("flush_total", "Total number of flushes", "gstdout")
	plug.param = fbit.Conf.String("param")
	plug.log = fbit.Logger

	return nil
}

func (plug gstdoutPlugin) Flush(ctx context.Context, ch <-chan plugin.Message) error {
	// Iterate Records
	count := 0

	for msg := range ch {
		plug.flushCounter.Add(1)
		plug.log.Debug("[gstdout] operation proceeded")

		// Print record keys and values
		fmt.Printf("[%d] %s: [%d.%d, {", count, msg.Tag(),
			msg.Time.Unix(), msg.Time.Nanosecond())
		rec := reflect.ValueOf(msg.Record)
		if rec.Kind() == reflect.Map {
			keyCount := 0
			for _, key := range rec.MapKeys() {
				if keyCount > 0 {
					fmt.Printf(", ")
				}
				strct := rec.MapIndex(key)
				fmt.Printf("\"%s\":\"%v\"", key.Interface(), strct.Interface())
				keyCount++
			}
		}
		fmt.Printf("}]\n")
		count++
	}

	return nil
}

func main() {}
