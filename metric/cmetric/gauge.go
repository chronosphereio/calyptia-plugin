package cmetric

import (
	"fmt"
	"time"

	cmetrics "github.com/calyptia/cmetrics-go"
)

type Gauge struct {
	Base    *cmetrics.Gauge
	OnError func(err error)
}

func (c *Gauge) Add(delta float64, labelValues ...string) {
	err := c.Base.Add(time.Now(), delta, labelValues)
	if err != nil && c.OnError != nil {
		c.OnError(fmt.Errorf("gauge add: %w", err))
	}
}

func (c *Gauge) Set(value float64, labelValues ...string) {
	err := c.Base.Set(time.Now(), value, labelValues)
	if err != nil && c.OnError != nil {
		c.OnError(fmt.Errorf("gauge set: %w", err))
	}
}

type noopGauge struct{}

func (n noopGauge) Add(float64, ...string) {}
func (n noopGauge) Set(float64, ...string) {}
