// Package plugin implements the global context and objects required to run an instance of a plugin
// also, the interfaces for input and output plugins.
package plugin

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/calyptia/plugin/metric"
)

// atomicUint32 is used to atomically check if the plugin has been registered.
var atomicUint32 uint32

var (
	theName   string
	theDesc   string
	theInput  InputPlugin
	theOutput OutputPlugin
	theCustom CustomPlugin
)

var (
	registerWG sync.WaitGroup
	initWG     sync.WaitGroup
	runCtx     context.Context
	runCancel  context.CancelFunc
	theChannel chan Message
)

func init() {
	registerWG.Add(1)
	theChannel = nil
}

type Fluentbit struct {
	Conf    ConfigLoader
	Metrics Metrics
	Logger  Logger
}

// InputPlugin interface to represent an input fluent-bit plugin.
type InputPlugin interface {
	Init(ctx context.Context, fbit *Fluentbit) error
	Collect(ctx context.Context, ch chan<- Message) error
}

// OutputPlugin interface to represent an output fluent-bit plugin.
type OutputPlugin interface {
	Init(ctx context.Context, fbit *Fluentbit) error
	Flush(ctx context.Context, ch <-chan Message) error
}

// CustomPlugin interface to represent an input fluent-bit plugin.
type CustomPlugin interface {
	Init(ctx context.Context, fbit *Fluentbit) error
}

// ConfigLoader interface to represent a fluent-bit configuration loader.
type ConfigLoader interface {
	String(key string) string
}

// Logger interface to represent a fluent-bit logging mechanism.
type Logger interface {
	Error(format string, a ...any)
	Warn(format string, a ...any)
	Info(format string, a ...any)
	Debug(format string, a ...any)
}

// Metrics builder.
type Metrics interface {
	NewCounter(name, desc string, labelValues ...string) metric.Counter
	NewGauge(name, desc string, labelValues ...string) metric.Gauge
}

// Message struct to store a fluent-bit message this is collected (input) or flushed (output)
// from a plugin implementation.
type Message struct {
	Time time.Time
	// Record should be a map or a struct.
	Record any
	tag    *string
}

// Tag is available at output.
func (m Message) Tag() string {
	if m.tag == nil {
		return ""
	}
	return *m.tag
}

// mustOnce allows to be called only once otherwise it panics.
// This is used to register a single plugin per file.
func mustOnce() {
	if atomic.LoadUint32(&atomicUint32) == 1 {
		panic("plugin already registered")
	}

	atomic.StoreUint32(&atomicUint32, 1)
}

// RegisterInput plugin.
// This function must be called only once per file.
func RegisterInput(name, desc string, in InputPlugin) {
	mustOnce()
	theName = name
	theDesc = desc
	theInput = in
}

// UnregisterInput unregisters the input plugin.
func UnregisterInput() {
	if theInput == nil {
		return
	}
	atomic.StoreUint32(&atomicUint32, 0)
	theName = ""
	theDesc = ""
	theInput = nil
}

// RegisterOutput plugin.
// This function must be called only once per file.
func RegisterOutput(name, desc string, out OutputPlugin) {
	mustOnce()
	theName = name
	theDesc = desc
	theOutput = out
}

// UnregisterOutput unregisters the output plugin.
func UnregisterOutput() {
	if theOutput == nil {
		return
	}
	atomic.StoreUint32(&atomicUint32, 0)
	theName = ""
	theDesc = ""
	theOutput = nil
}

// RegisterCustom plugin.
// This function must be called only once per file.
func RegisterCustom(name, desc string, custom CustomPlugin) {
	mustOnce()
	theName = name
	theDesc = desc
	theCustom = custom
}

// UnregisterCustom unregisters the custom plugin.
func UnregisterCustom() {
	if theCustom == nil {
		return
	}
	atomic.StoreUint32(&atomicUint32, 0)
	theName = ""
	theDesc = ""
	theCustom = nil
}
