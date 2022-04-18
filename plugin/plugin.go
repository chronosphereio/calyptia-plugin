package plugin

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// atomicUint32 is used to atomically check if the plugin has been registered.
var atomicUint32 uint32

var (
	theName   string
	theDesc   string
	theInput  InputPlugin
	theOutput OutputPlugin
)

var initWG sync.WaitGroup
var setupWG sync.WaitGroup
var once sync.Once
var runCtx context.Context
var runCancel context.CancelFunc
var theChannel chan Message

func init() {
	initWG.Add(1)
	setupWG.Add(1)
}

type InputPlugin interface {
	Setup(ctx context.Context, conf ConfigLoader) error
	Run(ctx context.Context, ch chan<- Message) error
}

type OutputPlugin interface {
	Setup(ctx context.Context, conf ConfigLoader) error
	Run(ctx context.Context, tag string, ch <-chan Message) error
}

type ConfigLoader interface {
	Load(key string) string
}

type Message struct {
	Time   time.Time
	Record map[string]string
}

type Writer interface {
	Write(ctx context.Context, t time.Time, rec map[string]string) error
}

type Reader interface {
	Read(ctx context.Context) (t time.Time, rec map[string]string, err error)
}

// mustOnce allows to be called only once otherwise it panics.
// This is used to register a single plugin per file.
func mustOnce() {
	if atomic.LoadUint32(&atomicUint32) == 1 {
		panic("plugin already registered")
	}

	atomic.StoreUint32(&atomicUint32, 1)
}

// RegisterInput registers a input plugin.
// This function must be called only once per file.
func RegisterInput(name, desc string, in InputPlugin) {
	mustOnce()
	theName = name
	theDesc = desc
	theInput = in
}

// RegisterOutput registers a output plugin.
// This function must be called only once per file.
func RegisterOutput(name, desc string, out OutputPlugin) {
	mustOnce()
	theName = name
	theDesc = desc
	theOutput = out
}
