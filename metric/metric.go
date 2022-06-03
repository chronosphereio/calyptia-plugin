// Package metric provides with Counter and Gauge interfaces.
// See /cmetric for an implementation using shared memory to cmetrics library.
package metric

// Counter describes a metric that accumulates values monotonically.
type Counter interface {
	Add(delta float64, labelValues ...string)
}

// Gauge describes a metric that takes specific values over time.
type Gauge interface {
	Add(delta float64, labelValues ...string)
	Set(value float64, labelValues ...string)
}
