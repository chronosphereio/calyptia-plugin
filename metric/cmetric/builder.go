package cmetric

import (
	"fmt"

	cmetrics "github.com/calyptia/cmetrics-go"
	"github.com/calyptia/plugin/metric"
)

type Builder struct {
	Namespace string
	SubSystem string
	Context   *cmetrics.Context
	OnError   func(err error)
}

func (b *Builder) NewCounter(name, desc string, labelValues ...string) metric.Counter {
	base, err := b.Context.CounterCreate(b.Namespace, b.SubSystem, name, desc, labelValues)
	if err != nil {
		if b.OnError != nil {
			b.OnError(fmt.Errorf("new counter: %w", err))
		}
		return noopCounter{}
	}

	return &Counter{
		Base:    base,
		OnError: b.OnError,
	}
}

func (b *Builder) NewGauge(name, desc string, labelValues ...string) metric.Gauge {
	base, err := b.Context.GaugeCreate(b.Namespace, b.SubSystem, name, desc, labelValues)
	if err != nil {
		if b.OnError != nil {
			b.OnError(fmt.Errorf("new gauge: %w", err))
		}
		return noopGauge{}
	}

	return &Gauge{
		Base:    base,
		OnError: b.OnError,
	}
}
