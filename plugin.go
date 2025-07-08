// Package plugin implements the global context and objects required to run an instance of a plugin
// also, the interfaces for input and output plugins.
package plugin

import "C"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/calyptia/cmetrics-go"
	"github.com/calyptia/plugin/input"
	"github.com/calyptia/plugin/metric"
)

var (
	// registerWG blocks until FLBPluginRegister has successfully run.
	registerWG sync.WaitGroup

	// Current plugin metadata. This is nil if the plugin has not been registered yet.
	// This is set by the RegisterInput, RegisterOutput, or RegisterCustom functions.
	pluginMeta     atomic.Pointer[pluginMetadata]
	unregisterFunc atomic.Pointer[func()]

	// currInstanceMu guards access to currInstance.
	currInstanceMu sync.Mutex
	// Current instance of the plugin, used by functions called from fluent-bit like FLBPluginInit.
	currInstance *pluginInstance
)

func init() {
	// Require FLBPluginRegister to be called before any other fluent-bit-facing functions execute.
	registerWG.Add(1)
}

type instanceState string

const (
	instanceStateCreated     instanceState = ""
	instanceStateInitialized instanceState = "initialized"
	instanceStateRunnable    instanceState = "runnable"
	instanceStatePreExit     instanceState = "preExit"
)

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

// RegisterInput plugin.
// This function must be called only once per file.
func RegisterInput(name, desc string, in InputPlugin) {
	if !pluginMeta.CompareAndSwap(nil, &pluginMetadata{
		name:  name,
		desc:  desc,
		input: in,
	}) {
		panic("plugin already registered")
	}
}

// RegisterOutput plugin.
// This function must be called only once per file.
func RegisterOutput(name, desc string, out OutputPlugin) {
	if !pluginMeta.CompareAndSwap(nil, &pluginMetadata{
		name:   name,
		desc:   desc,
		output: out,
	}) {
		panic("plugin already registered")
	}
}

// RegisterCustom plugin.
// This function must be called only once per file.
func RegisterCustom(name, desc string, custom CustomPlugin) {
	if !pluginMeta.CompareAndSwap(nil, &pluginMetadata{
		name:   name,
		desc:   desc,
		custom: custom,
	}) {
		panic("plugin already registered")
	}
}

// pluginMeta describes a plugin and exposes hooks into its implementation.
type pluginMetadata struct {
	name string
	desc string

	// Exactly one of the following will be set:
	input  InputPlugin
	output OutputPlugin
	custom CustomPlugin
}

type cmetricsContextProvider func(plugin unsafe.Pointer) (*cmetrics.Context, error)
type configLoaderProvider func(plugin unsafe.Pointer) ConfigLoader

func newPluginInstance(meta pluginMetadata) *pluginInstance {
	return &pluginInstance{
		meta:                meta,
		cmetricsCtxProvider: input.FLBPluginGetCMetricsContext,
		configLoaderProvider: func(ptr unsafe.Pointer) ConfigLoader {
			if meta.input != nil {
				return &flbInputConfigLoader{ptr: ptr}
			} else if meta.output != nil {
				return &flbOutputConfigLoader{ptr: ptr}
			} else if meta.custom != nil {
				return &flbCustomConfigLoader{ptr: ptr}
			}
			return nil
		},
		state:               instanceStateCreated,
		maxBufferedMessages: defaultMaxBufferedMessages,
	}
}

// pluginInstance is an instance of a plugin.
type pluginInstance struct {
	meta                 pluginMetadata
	cmetricsCtxProvider  cmetricsContextProvider
	configLoaderProvider configLoaderProvider
	runningWG            sync.WaitGroup // Number of running preRun and callback methods.

	// mu protects all members below.
	// It is generally held during state checks and transitions but not during long-running callbacks.
	mu                  sync.Mutex
	state               instanceState
	runCtx              context.Context
	runCancel           context.CancelFunc
	msgChannel          chan Message
	maxBufferedMessages int
}

// withCMetricsContextProvider overrides the cmetricsContextProvider.
// This must be called immediately after creating the instance.
func (p *pluginInstance) withCMetricsContextProvider(provider cmetricsContextProvider) *pluginInstance {
	p.cmetricsCtxProvider = provider
	return p
}

// withConfigLoaderProvider overrides the configLoaderProvider.
// This must be called immediately after creating the instance.
func (p *pluginInstance) withConfigLoaderProvider(provider configLoaderProvider) *pluginInstance {
	p.configLoaderProvider = provider
	return p
}

// init initializes a newly created plugin instance.
// This returns an error if the plugin if not in a new state or cannot initialize.
// For input and output plugins, this moves the plugin to an initialized state unless there is an init error.
// For custom plugins, this moves the plugin to a runnable state unless there is an init error.
func (p *pluginInstance) init(ptr unsafe.Pointer) (initErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state != instanceStateCreated {
		return fmt.Errorf("unexpected plugin state %q", p.state)
	}

	newState := instanceStateInitialized
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if initErr != nil {
			cancel()
			p.runCtx = nil
			p.runCancel = nil

			return
		}
		p.state = newState
	}()

	if p.meta.input != nil {
		defer cancel()
		cmt, err := p.cmetricsCtxProvider(ptr)
		if err != nil {
			return err
		}

		cfgLoader := p.configLoaderProvider(ptr)
		fbit := &Fluentbit{
			Conf:    cfgLoader,
			Metrics: makeMetrics(cmt),
			Logger:  &flbInputLogger{ptr: ptr},
		}

		if maxBufferedStr := cfgLoader.String("go.MaxBufferedMessages"); maxBufferedStr != "" {
			maxBuffered, err := strconv.Atoi(maxBufferedStr)
			if err != nil {
				return fmt.Errorf("go.MaxBufferedMessages must be an integer, got %q", maxBufferedStr)
			}

			p.maxBufferedMessages = maxBuffered
		}

		if err := p.meta.input.Init(ctx, fbit); err != nil {
			return fmt.Errorf("initializing plugin %q: %w", p.meta.name, err)
		}
	} else if p.meta.output != nil {
		defer cancel()
		cmt, err := p.cmetricsCtxProvider(ptr)
		if err != nil {
			return err
		}

		fbit := &Fluentbit{
			Conf:    p.configLoaderProvider(ptr),
			Metrics: makeMetrics(cmt),
			Logger:  &flbOutputLogger{ptr: ptr},
		}

		if err := p.meta.output.Init(ctx, fbit); err != nil {
			return fmt.Errorf("initializing plugin %q: %w", p.meta.name, err)
		}
	} else {
		// Custom plugins don't have preInit functions that set context, so they are immediately runnable
		p.runCtx, p.runCancel = ctx, cancel
		p.state = instanceStateRunnable

		fbit := &Fluentbit{
			Conf:    p.configLoaderProvider(ptr),
			Metrics: nil,
			Logger:  &flbOutputLogger{ptr: ptr},
		}

		if err := p.meta.custom.Init(ctx, fbit); err != nil {
			return fmt.Errorf("initializing plugin %q: %w", p.meta.name, err)
		}
	}

	return nil
}

// inputPreRun transitions an initialized input plugin into runnable state.
func (p *pluginInstance) inputPreRunWithLock() error {
	// Only input plugins have a pre-run step
	if p.meta.input == nil {
		return nil
	}

	if p.state != instanceStateInitialized {
		return fmt.Errorf("invalid plugin state %q", p.state)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	p.runCtx = runCtx
	p.runCancel = runCancel
	p.msgChannel = make(chan Message, p.maxBufferedMessages)
	p.state = instanceStateRunnable

	p.runningWG.Add(1)
	go func() {
		defer p.runningWG.Done()

		err := p.meta.input.Collect(runCtx, p.msgChannel)
		if err != nil {
			fmt.Fprintf(os.Stderr, "collect error: %v\n", err)
		}
	}()

	go func() {
		<-runCtx.Done()

		log.Printf("goroutine will be stopping: name=%q\n", p.meta.name)
	}()

	return nil
}

// outputPreRun transitions an output plugin into runnable state.
func (p *pluginInstance) outputPreRunWithLock() error {
	// Only input plugins have a pre-run step
	if p.meta.output == nil {
		return fmt.Errorf("plugin is not an output plugin")
	}

	if p.state != instanceStateInitialized {
		return fmt.Errorf("invalid plugin state %q", p.state)
	}

	p.runCtx, p.runCancel = context.WithCancel(context.Background())
	p.msgChannel = make(chan Message)
	p.state = instanceStateRunnable

	p.runningWG.Add(1)
	go func() {
		defer p.runningWG.Done()
		if err := p.meta.output.Flush(p.runCtx, p.msgChannel); err != nil {
			fmt.Fprintf(os.Stderr, "FLBPluginOutputPreRun error: %v\n", err)
		}
	}()

	go func() {
		<-p.runCtx.Done()

		log.Printf("goroutine will be stopping: name=%q\n", p.meta.name)
	}()

	return nil
}

// inputCallback consumes up to maxBufferedMessages message from the plugin's message channel,
// returning early if the plugin is shutdown.
// Consumed messages are marshaled into msgpack bytes and the contents and length of contents
// set in the respective data and csize input variables.
func (p *pluginInstance) inputCallback(data *unsafe.Pointer, csize *C.size_t) int {
	p.mu.Lock()
	if p.state != instanceStateRunnable {
		p.mu.Unlock()
		return input.FLB_RETRY
	}

	p.runningWG.Add(1)
	defer p.runningWG.Done()
	p.mu.Unlock()

	buf := bytes.NewBuffer([]byte{})

	for loop := min(len(p.msgChannel), p.maxBufferedMessages); loop > 0; loop-- {
		select {
		case msg, ok := <-p.msgChannel:
			if !ok {
				return input.FLB_ERROR
			}

			b, err := msgpack.Marshal([]any{&EventTime{msg.Time}, msg.Record})
			if err != nil {
				fmt.Fprintf(os.Stderr, "msgpack marshal: %s\n", err)
				return input.FLB_ERROR
			}

			buf.Grow(len(b))
			buf.Write(b)
		case <-p.runCtx.Done():
			err := p.runCtx.Err()
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(os.Stderr, "run: %s\n", err)
				return input.FLB_ERROR
			}
			// enforce a runtime gc, to prevent the thread finalizer on
			// fluent-bit to kick in before any remaining data has not been GC'ed
			// causing a sigsegv.
			defer runtime.GC()
			loop = 0 // Exit the for loop on plugin shutdown
		default:
			loop = 0 // Exit the for loop if there are no messages to consume
		}
	}

	if buf.Len() > 0 {
		b := buf.Bytes()
		cdata := C.CBytes(b)
		*data = cdata
		if csize != nil {
			*csize = C.size_t(len(b))
		}
	}

	return input.FLB_OK
}

// outputFlush writes the messages in msgpackBytes to the plugin's message channel,
// returning early if the plugin is shutdown.
func (p *pluginInstance) outputFlush(tag string, msgpackBytes []byte) error {
	p.mu.Lock()
	if p.state != instanceStateRunnable {
		p.mu.Unlock()
		return fmt.Errorf("invalid plugin state %q", p.state)
	}

	p.runningWG.Add(1)
	defer p.runningWG.Done()
	p.mu.Unlock()

	dec := msgpack.NewDecoder(bytes.NewReader(msgpackBytes))
	for {
		select {
		case <-p.runCtx.Done():
			err := p.runCtx.Err()
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(os.Stderr, "run: %s\n", err)
				return fmt.Errorf("run: %w", err)
			}

			return nil
		default:
		}

		msg, err := decodeMsg(dec, tag)
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		p.msgChannel <- msg
	}
}

// stop stops the plugin, freeing resources and returning it to initialized state.
// Calling stop will signal callbacks to return via the plugin's context, then wait for
// callbacks to finish and return before freeing resources and returning.
func (p *pluginInstance) stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state != instanceStateRunnable && p.state != instanceStatePreExit {
		return nil
	}

	if p.runCancel != nil {
		p.runCancel()
	}

	p.runningWG.Wait()

	if p.msgChannel != nil {
		close(p.msgChannel)
	}

	p.state = instanceStateInitialized
	p.runCtx = nil
	p.runCancel = nil
	p.msgChannel = nil

	return nil
}

// resume restarts plugins, running pre-run functions for input and output plugins.
func (p *pluginInstance) resume() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state != instanceStateInitialized {
		return fmt.Errorf("invalid plugin state %q", p.state)
	}

	if p.meta.input != nil {
		if err := p.inputPreRunWithLock(); err != nil {
			return err
		}
	} else if p.meta.output != nil {
		if err := p.outputPreRunWithLock(); err != nil {
			return err
		}
	}

	p.state = instanceStateRunnable

	return nil
}

// stop prepares to stop an output plugin.
// Calling outputPreExit will signal callbacks to return via the plugin's context, then wait for
// callbacks to finish and return before freeing resources and returning.
func (p *pluginInstance) outputPreExit() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Only output plugins have a pre-exit step
	if p.meta.output == nil {
		return fmt.Errorf("plugin is not an output plugin")
	}

	if p.state != instanceStateRunnable {
		return fmt.Errorf("invalid plugin state %q", p.state)
	}

	p.runCancel()

	// Wait for any running callback/flush to finish before closing the message channel
	p.runningWG.Wait()
	close(p.msgChannel)

	p.state = instanceStatePreExit
	p.runCtx = nil
	p.runCancel = nil
	p.msgChannel = nil

	return nil
}

// flbReturnCode returns a fluent-bit C int enum value indicating function success.
func flbReturnCode(err error) int {
	if err == nil {
		return input.FLB_OK
	}

	return input.FLB_ERROR
}
