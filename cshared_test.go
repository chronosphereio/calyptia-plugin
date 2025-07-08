package plugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/calyptia/cmetrics-go"
	"github.com/calyptia/plugin/input"
	"github.com/calyptia/plugin/metric"
)

func newTestInputInstance(t testing.TB, input InputPlugin) *pluginInstance {
	inst := pluginInstanceWithFakes(newPluginInstance(pluginMetadata{
		name:  "test-plugin",
		desc:  "test plugin",
		input: input,
	}))
	t.Cleanup(func() {
		stopErr := make(chan error)
		go func() {
			stopErr <- inst.stop()
		}()

		select {
		case err := <-stopErr:
			assert.NoError(t, err)
			return
		case <-time.After(time.Second):
			require.Fail(t, "timed out waiting for instance to stop")
		}
	})

	return inst
}

func newTestOutputInstance(t testing.TB, output OutputPlugin) *pluginInstance {
	inst := pluginInstanceWithFakes(newPluginInstance(pluginMetadata{
		name:   "test-plugin",
		desc:   "test plugin",
		output: output,
	}))
	t.Cleanup(func() { assert.NoError(t, inst.stop()) })
	return inst
}

func pluginInstanceWithFakes(inst *pluginInstance) *pluginInstance {
	return inst.
		withCMetricsContextProvider(func(_ unsafe.Pointer) (*cmetrics.Context, error) {
			return cmetrics.NewContext()
		}).
		withConfigLoaderProvider(func(_ unsafe.Pointer) ConfigLoader {
			return fakeConfigLoader{}
		})
}

func TestInputCallbackLifecycle(t *testing.T) {
	plugin := newTestInputPlugin()
	inst := newTestInputInstance(t, plugin)

	// Initialization
	require.NoError(t, inst.init(nil))
	require.Equal(t, int64(1), plugin.initCount.Load())

	require.ErrorContains(t, inst.init(nil), `unexpected plugin state "initialized"`)
	require.Equal(t, int64(1), plugin.initCount.Load(), "initialization should only run once")

	// Early attempt to callback
	_, callbackResp := testCallback(inst)
	require.Equal(t, input.FLB_RETRY, callbackResp, "pre-run must be called before callback")

	// Pre-run
	require.NoError(t, inst.resume())
	require.Eventually(t, plugin.collectRunning.Load, time.Second, time.Millisecond,
		"collect background loop should have started running")
	m1 := testMessage(map[string]any{"name": "m1"})
	m2 := testMessage(map[string]any{"name": "m2"})
	plugin.enqueue(m1)()
	plugin.enqueue(m2)()

	require.ErrorContains(t, inst.resume(), `invalid plugin state "runnable"`)

	// Callback
	callbackBytes, callbackResp := testCallback(inst)
	require.Equal(t, input.FLB_OK, callbackResp)
	require.Equal(t, []Message{m1, m2}, decodeMessages(t, callbackBytes))
	require.True(t, plugin.collectRunning.Load())

	// Stop (ensuring collect loop exits cleanly)
	plugin.onCollectDone = func(ch chan<- Message) {
		// Keep enqueueing after stop to ensure the plugin message channel wasn't closed early
		time.Sleep(10 * time.Millisecond)
		ch <- testMessage(map[string]any{"name": "m3"})
	}
	require.NoError(t, inst.stop())
	require.False(t, plugin.collectRunning.Load())
	require.NoError(t, inst.stop(), "stop should be idempotent")

	callbackBytes, callbackResp = testCallback(inst)
	require.Equal(t, input.FLB_RETRY, callbackResp)
	assert.Empty(t, callbackBytes)

	// Resume stopped pipeline
	require.NoError(t, inst.resume())
	require.ErrorContains(t, inst.resume(), `invalid plugin state "runnable"`)
	callbackBytes, callbackResp = testCallback(inst)
	require.Equal(t, input.FLB_OK, callbackResp)
	assert.Empty(t, callbackBytes, "m3 message from earlier not dequeued")
	require.Eventually(t, plugin.collectRunning.Load, time.Second, time.Millisecond,
		"collect background loop should have started running")
	m4 := testMessage(map[string]any{"name": "m4"})
	plugin.enqueue(m4)()
	callbackBytes, callbackResp = testCallback(inst)
	require.Equal(t, input.FLB_OK, callbackResp)
	require.Equal(t, []Message{m4}, decodeMessages(t, callbackBytes))

	// Stop again
	require.NoError(t, inst.stop())
	require.False(t, plugin.collectRunning.Load())
}

// testMessage returns a Message with the given record map and current timestamp.
func testMessage(record map[string]any) Message {
	tag := ""
	return Message{
		Time:   time.Now().UTC(),
		Record: record,
		tag:    &tag,
	}
}

func newTestInputPlugin() *testInputPlugin {
	return &testInputPlugin{
		inputs: make(chan *collectMessage),
	}
}

// testInputPlugin is an InputPlugin used to help test plugin callback and concurrency behavior.
type testInputPlugin struct {
	initCount      atomic.Int64            // Count of calls to Init method.
	collectRunning atomic.Bool             // Indicates whether the Collect method is running.
	onCollectDone  func(ch chan<- Message) // Settable callback invoked when Collect is about to return.

	inputs chan *collectMessage
}

var _ InputPlugin = (*testInputPlugin)(nil)

func (t *testInputPlugin) Init(ctx context.Context, fbit *Fluentbit) error {
	t.initCount.Add(1)
	return nil
}

func (t *testInputPlugin) Collect(ctx context.Context, ch chan<- Message) error {
	t.collectRunning.Store(true)
	defer t.collectRunning.Store(false)

	for {
		select {
		case m := <-t.inputs:
			ch <- m.msg
			m.collectedWG.Done()
		case <-ctx.Done():
			if t.onCollectDone != nil {
				t.onCollectDone(ch)
			}
			return nil
		}
	}
}

// enqueue the message m to be processed by Collect. When called, the returned function
// blocks until a running Collect puts m on the plugin's input channel.
func (t *testInputPlugin) enqueue(m Message) (waitForCollected func()) {
	cm := &collectMessage{msg: m}
	cm.collectedWG.Add(1)
	t.inputs <- cm

	return cm.collectedWG.Wait
}

// collectMessage is a helper wrapper used by testInputPlugin that wraps a Message.
type collectMessage struct {
	msg         Message
	collectedWG sync.WaitGroup // Decremented to 0 when testInputPlugin Collect processes the message.
}

func decodeMessages(t testing.TB, msgpackBytes []byte) []Message {
	var messages []Message

	dec := msgpack.NewDecoder(bytes.NewReader(msgpackBytes))
	for {
		msg, err := decodeMsg(dec, "")
		if errors.Is(err, io.EOF) {
			return messages
		}
		require.NoError(t, err)

		messages = append(messages, msg)
	}
}

type testPluginInputCallbackCtrlC struct{}

func (t testPluginInputCallbackCtrlC) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t testPluginInputCallbackCtrlC) Collect(ctx context.Context, ch chan<- Message) error {
	return nil
}

func TestInputCallbackCtrlC(t *testing.T) {
	inst := newTestInputInstance(t, testPluginInputCallbackCtrlC{})

	require.NoError(t, inst.init(nil))
	require.NoError(t, inst.resume())

	cdone := make(chan struct{})
	timeout := time.After(1 * time.Second)

	go func() {
		testCallback(inst)
		close(cdone)
	}()

	select {
	case <-cdone:
		inst.runCancel()
	case <-timeout:
		t.Fatalf("timed out ...")
	}
}

type testPluginInputCallbackDangle struct {
	calls atomic.Int64
}

func (t *testPluginInputCallbackDangle) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t *testPluginInputCallbackDangle) Collect(ctx context.Context, ch chan<- Message) error {
	t.calls.Add(1)
	ch <- Message{
		Time: time.Now(),
		Record: map[string]string{
			"Foo": "BAR",
		},
	}
	return nil
}

// TestInputCallbackDangle assures the API will not attempt to invoke
// Collect multiple times. This is inline with backward-compatible
// behavior.
func TestInputCallbackDangle(t *testing.T) {
	input := &testPluginInputCallbackDangle{}
	inst := newTestInputInstance(t, input)

	cdone := make(chan struct{})
	ptr := unsafe.Pointer(nil)

	// prepare channel for input explicitly.
	require.NoError(t, inst.init(ptr))
	require.NoError(t, inst.resume())

	go func() {
		ticker := time.NewTicker(collectInterval)
		defer ticker.Stop()

		testCallback(inst)
		for {
			select {
			case <-ticker.C:
				testCallback(inst)
			case <-cdone:
				return
			}
		}
	}()

	time.Sleep(5 * time.Second)

	inst.runCancel()
	close(cdone)

	// Test the assumption that only a single goroutine is ingesting records.
	require.EqualValues(t, 1, input.calls.Load())
}

type testPluginInputCallbackInfinite struct {
	calls atomic.Int64
}

func (t *testPluginInputCallbackInfinite) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t *testPluginInputCallbackInfinite) Collect(ctx context.Context, ch chan<- Message) error {
	t.calls.Add(1)
	for {
		select {
		default:
			ch <- Message{
				Time: time.Now(),
				Record: map[string]string{
					"Foo": "BAR",
				},
			}
		// for tests to correctly pass our infinite loop needs
		// to return once the context has been finished.
		case <-ctx.Done():
			return nil
		}
	}
}

// TestInputCallbackInfinite is a test for the main method most plugins
// use where they do not return from the first invocation of collect.
func TestInputCallbackInfinite(t *testing.T) {
	input := &testPluginInputCallbackInfinite{}
	inst := newTestInputInstance(t, input)

	cdone := make(chan struct{})
	cshutdown := make(chan struct{})
	ptr := unsafe.Pointer(nil)

	// prepare channel for input explicitly.
	require.NoError(t, inst.init(ptr))
	require.NoError(t, inst.resume())

	go func() {
		ticker := time.NewTicker(collectInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				testCallback(inst)
				if ptr != nil {
					close(cdone)
					return
				}
			case <-cshutdown:
				return
			}
		}
	}()

	timeout := time.After(10 * time.Second)

	select {
	case <-cdone:
		inst.runCancel()
		// make sure Collect is not being invoked after Done().
		time.Sleep(collectInterval * 10)
		// Test the assumption that only a single goroutine is ingesting records.
		require.EqualValues(t, 1, input.calls.Load())
	case <-timeout:
		inst.runCancel()
		close(cshutdown)
		// This test seems to fail somewhat frequently because the Collect goroutine
		// inside cshared is never being scheduled.
		t.Fatalf("timed out ...")
	}
}

type testPluginInputCallbackLatency struct{}

func (t testPluginInputCallbackLatency) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t testPluginInputCallbackLatency) Collect(ctx context.Context, ch chan<- Message) error {
	tick := time.NewTimer(time.Second * 1)
	for {
		select {
		case <-tick.C:
			for i := 0; i < 128; i++ {
				ch <- Message{
					Time: time.Now(),
					Record: map[string]string{
						"Foo": "BAR",
					},
				}
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// TestInputCallbackInfiniteLatency is a test of the latency between
// messages.
func TestInputCallbackLatency(t *testing.T) {
	input := &testPluginInputCallbackLatency{}
	inst := newTestInputInstance(t, input)

	cdone := make(chan struct{})
	cstarted := make(chan struct{})
	cmsg := make(chan []byte)

	// prepare channel for input explicitly.
	require.NoError(t, inst.init(nil))
	require.NoError(t, inst.resume())

	go func() {
		ticker := time.NewTicker(collectInterval)
		defer ticker.Stop()

		buf, _ := testCallback(inst)
		if len(buf) > 0 {
			cmsg <- buf
		}

		close(cstarted)
		for {
			select {
			case <-cdone:
				t.Log("---- collect done")
				return
			case <-ticker.C:
				buf, _ := testCallback(inst)
				if len(buf) > 0 {
					cmsg <- buf
				}
			}
		}
	}()

	<-cstarted
	t.Log("---- started")
	timeout := time.After(5 * time.Second)
	msgs := 0

	for {
		select {
		case buf := <-cmsg:
			dec := msgpack.NewDecoder(bytes.NewReader(buf))
			for {
				msg, err := decodeMsg(dec, "test-tag")
				if errors.Is(err, io.EOF) {
					break
				}

				if err != nil {
					t.Fatalf("decode error: %v", err)
				}

				msgs++

				if time.Since(msg.Time) > time.Millisecond*5 {
					t.Errorf("latency too high: %fms",
						float64(time.Since(msg.Time)/time.Millisecond))
				}
			}
		case <-timeout:
			inst.runCancel()
			close(cdone)

			if msgs < 128 {
				t.Fatalf("too few messages: %d", msgs)
			}
			return
		}
	}
}

type testInputCallbackInfiniteConcurrent struct{}

var (
	concurrentWait        sync.WaitGroup
	concurrentCountStart  atomic.Int64
	concurrentCountFinish atomic.Int64
)

func (t testInputCallbackInfiniteConcurrent) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t testInputCallbackInfiniteConcurrent) Collect(ctx context.Context, ch chan<- Message) error {
	fmt.Printf("---- infinite concurrent collect\n")

	for i := 0; i < 64; i++ {
		go func(ch chan<- Message, id int) {
			fmt.Printf("---- infinite concurrent started: %d\n", id)
			concurrentCountStart.Add(1)
			ch <- Message{
				Time: time.Now(),
				Record: map[string]string{
					"ID": fmt.Sprintf("%d", id),
				},
			}
			concurrentCountFinish.Add(1)
			concurrentWait.Done()
			fmt.Printf("---- infinite concurrent finished: %d\n", id)
		}(ch, i)
		fmt.Printf("---- infinite concurrent starting: %d\n", i)
	}
	// for tests to correctly pass our infinite loop needs
	// to return once the context has been finished.
	<-ctx.Done()

	return nil
}

// TestInputCallbackInfiniteConcurrent is meant to make sure we do not
// break anythin with respect to concurrent ingest.
func TestInputCallbackInfiniteConcurrent(t *testing.T) {
	input := &testInputCallbackInfiniteConcurrent{}
	inst := newTestInputInstance(t, input)

	cdone := make(chan struct{})
	cstarted := make(chan struct{})
	ptr := unsafe.Pointer(nil)

	concurrentWait.Add(64)

	// prepare channel for input explicitly.
	require.NoError(t, inst.init(ptr))
	require.NoError(t, inst.resume())

	go func() {
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()

		testCallback(inst)
		close(cstarted)

		for {
			select {
			case <-ticker.C:
				testCallback(inst)
			case <-inst.runCtx.Done():
				return
			}
		}
	}()

	go func() {
		concurrentWait.Wait()
		close(cdone)
	}()

	<-cstarted
	timeout := time.After(10 * time.Second)

	select {
	case <-cdone:
		inst.runCancel()
	case <-timeout:
		inst.runCancel()
		// this test seems to timeout semi-frequently... need to get to
		// the bottom of it...
		t.Fatalf("---- timed out: %d/%d ...",
			concurrentCountStart.Load(),
			concurrentCountFinish.Load())
	}
}

type testOutputHandlerReflect struct {
	param        string
	flushCounter metric.Counter
	log          Logger
	Test         *testing.T
	Check        func(t *testing.T, msg Message) error
}

func (plug *testOutputHandlerReflect) Init(ctx context.Context, fbit *Fluentbit) error {
	plug.flushCounter = fbit.Metrics.NewCounter("flush_total", "Total number of flushes", "gstdout")
	plug.param = fbit.Conf.String("param")
	plug.log = fbit.Logger

	return nil
}

func (plug *testOutputHandlerReflect) Flush(ctx context.Context, ch <-chan Message) error {
	plug.Test.Helper()
	count := 0

	for {
		select {
		case msg := <-ch:
			rec := reflect.ValueOf(msg.Record)
			if rec.Kind() != reflect.Map {
				return fmt.Errorf("incorrect record type in flush")
			}

			if plug.Check != nil {
				if err := plug.Check(plug.Test, msg); err != nil {
					return err
				}
			}
			count++
		case <-ctx.Done():
			if count <= 0 {
				return fmt.Errorf("no records flushed")
			}
			return nil
		}
	}
}

type testOutputHandlerMapString struct {
	param        string
	flushCounter metric.Counter
	log          Logger
}

func (plug *testOutputHandlerMapString) Init(ctx context.Context, fbit *Fluentbit) error {
	plug.flushCounter = fbit.Metrics.NewCounter("flush_total", "Total number of flushes", "gstdout")
	plug.param = fbit.Conf.String("param")
	plug.log = fbit.Logger

	return nil
}

func (plug *testOutputHandlerMapString) Flush(ctx context.Context, ch <-chan Message) error {
	count := 0

	for {
		select {
		case msg := <-ch:
			record, ok := msg.Record.(map[string]interface{})
			if !ok {
				return fmt.Errorf("unable to convert record to map[string]")
			}
			for _, value := range record {
				_, ok := value.(string)
				if !ok {
					return fmt.Errorf("unable to convert value")
				}
			}
			count++
		case <-ctx.Done():
			if count <= 0 {
				return fmt.Errorf("no records flushed")
			}
			return nil
		}
	}
}

// TestOutput is a simple output test. It also shows which format of records
// we currently support and how they should be handled. Feel free to use this
// code as an example of how to implement the Flush receive for output plugins.
//
// At the moment all Message.Records will be sent as a `map[string]interface{}`.
// Older plugins will have to do as testOutputHandlerMapString.Flush does
// and cast the actual value as a string.
func TestOutputSimulated(t *testing.T) {
	var wg sync.WaitGroup
	ctxt, cancel := context.WithCancel(context.Background())
	ch := make(chan Message)
	tag := "tag"

	outputReflect := testOutputHandlerReflect{Test: t}

	wg.Add(1)
	go func(ctxt context.Context, wg *sync.WaitGroup, ch <-chan Message) {
		err := outputReflect.Flush(ctxt, ch)
		if err != nil {
			t.Error(err)
		}
		wg.Done()
	}(ctxt, &wg, ch)

	ch <- Message{
		Time: time.Now(),
		Record: map[string]interface{}{
			"foo": "bar",
			"bar": "1",
		},
		tag: &tag,
	}

	cancel()
	wg.Wait()
	wg = sync.WaitGroup{}
	ctxt, cancel = context.WithCancel(context.Background())

	outputMapString := testOutputHandlerMapString{}

	wg.Add(1)
	go func(ctxt context.Context, wg *sync.WaitGroup, ch <-chan Message) {
		err := outputMapString.Flush(ctxt, ch)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		wg.Done()
	}(ctxt, &wg, ch)

	ch <- Message{
		Time: time.Now(),
		Record: map[string]interface{}{
			"foo":    "bar",
			"foobar": "1",
		},
		tag: &tag,
	}

	cancel()
	wg.Wait()
	close(ch)
}

func TestOutputFlush(t *testing.T) {
	var wg sync.WaitGroup

	now := time.Now().UTC()

	out := &testOutputHandlerReflect{
		Test: t,
		Check: func(t *testing.T, msg Message) error {
			defer wg.Done()

			expectTag := "foobar"
			assert.Equal(t, Message{
				Time: now,
				Record: map[string]any{
					"foo":    "bar",
					"bar":    int8(3),
					"foobar": 1.337,
				},
				tag: &expectTag,
			}, msg)

			return nil
		},
	}
	inst := newTestOutputInstance(t, out)
	require.NoError(t, inst.init(nil))
	require.NoError(t, inst.resume())

	msg := Message{
		Time: now,
		Record: map[string]any{
			"foo":    "bar",
			"bar":    3,
			"foobar": 1.337,
		},
	}

	b, err := msgpack.Marshal([]any{
		&EventTime{msg.Time},
		msg.Record,
	})
	assert.NoError(t, err)

	wg.Add(1)
	assert.NoError(t, inst.outputFlush("foobar", b))
	wg.Wait()
}

type fakeConfigLoader map[string]string

var _ ConfigLoader = (fakeConfigLoader)(nil)

func (f fakeConfigLoader) String(key string) string {
	return f[key]
}
