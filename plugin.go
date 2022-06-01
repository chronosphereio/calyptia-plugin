// Package plugin implements the global context and objects required to run an instance of a plugin
// also, the interfaces for input and output plugins.
package plugin

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	cmetrics "github.com/calyptia/cmetrics-go"
)

// atomicUint32 is used to atomically check if the plugin has been registered.
var atomicUint32 uint32

var (
	theName   string
	theDesc   string
	theInput  InputPlugin
	theOutput OutputPlugin
)

var (
	registerWG sync.WaitGroup
	initWG     sync.WaitGroup
	once       sync.Once
	runCtx     context.Context
	runCancel  context.CancelFunc
	theChannel chan Message
)

func init() {
	registerWG.Add(1)
	initWG.Add(1)
}

// InputPlugin interface to represent an input fluent-bit plugin.
type InputPlugin interface {
	Init(ctx context.Context, conf ConfigLoader, cmt *cmetrics.Context) error
	Collect(ctx context.Context, ch chan<- Message) error
}

// OutputPlugin interface to represent an output fluent-bit plugin.
type OutputPlugin interface {
	Init(ctx context.Context, conf ConfigLoader, cmt *cmetrics.Context) error
	Flush(ctx context.Context, ch <-chan Message) error
}

// ConfigLoader interface to represent a fluent-bit configuration loader.
type ConfigLoader interface {
	String(key string) string
}

// Message struct to store a fluent-bit message this is collected (input) or flushed (output)
// from a plugin implementation.
type Message struct {
	Time   time.Time
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

// RegisterOutput plugin.
// This function must be called only once per file.
func RegisterOutput(name, desc string, out OutputPlugin) {
	mustOnce()
	theName = name
	theDesc = desc
	theOutput = out
}
