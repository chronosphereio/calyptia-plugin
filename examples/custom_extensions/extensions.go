package main

import (
	"context"
	"time"

	"github.com/calyptia/plugin"
)

func init() {
	plugin.RegisterCustom("extensions", "extensions GO!", &extensionsPlugin{})
}

type extensionsPlugin struct {
	log          plugin.Logger
}

const (
	defaultTimeoutSeconds = 10
)

func (plug *extensionsPlugin) Init(ctx context.Context, fbit *plugin.Fluentbit) error {
	plug.log = fbit.Logger

	plug.log.Debug("[custom-go] extensions = '%s'", fbit.Conf.String("extensions"))
	go func() {
		for {
			plug.log.Debug("[custom-go] Go extensions alive %v", time.Now())
			time.Sleep(defaultTimeoutSeconds * time.Second)
		}
	}()

	return nil
}

func main() {
}
