package cmetric

import (
	"fmt"
	"time"

	cmetrics "github.com/calyptia/cmetrics-go"
)

type Counter struct {
	Base    *cmetrics.Counter
	OnError func(err error)
}

func (c *Counter) Add(delta float64, labelValues ...string) {
	err := c.Base.Add(time.Now(), delta, labelValues)
	if err != nil && c.OnError != nil {
		c.OnError(fmt.Errorf("counter add: %w", err))
	}
}

type noopCounter struct{}

func (n noopCounter) Add(float64, ...string) {}
