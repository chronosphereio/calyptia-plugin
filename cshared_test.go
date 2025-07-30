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

	"github.com/alecthomas/assert/v2"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/calyptia/plugin/metric"
)

type testPluginInputCallbackCtrlC struct{}

func (t testPluginInputCallbackCtrlC) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t testPluginInputCallbackCtrlC) Collect(ctx context.Context, ch chan<- Message) error {
	return nil
}

func init() {
	registerWG.Done()
}

func TestMain(m *testing.M) {
	defer flbPluginReset()
	m.Run()
}

func TestInputCallbackCtrlC(t *testing.T) {
	theInputLock.Lock()
	theInput = testPluginInputCallbackCtrlC{}
	theInputLock.Unlock()

	cdone := make(chan bool)
	timeout := time.NewTimer(1 * time.Second)
	defer timeout.Stop()

	// #nosec G103 creating a NULL pointer should be fine.
	ptr := unsafe.Pointer(nil)

	// prepare channel for input explicitly.
	prepareInputCollector(false)

	go func() {
		FLBPluginInputCallback(&ptr, nil)
		cdone <- true
	}()

	select {
	case <-cdone:
		timeout.Stop()
		runCancel()
	case <-timeout.C:
		t.Fatalf("timed out ...")
	}
}

var testPluginInputCallbackDangleFuncs atomic.Int64

type testPluginInputCallbackDangle struct{}

func (t testPluginInputCallbackDangle) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t testPluginInputCallbackDangle) Collect(ctx context.Context, ch chan<- Message) error {
	testPluginInputCallbackDangleFuncs.Add(1)
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
	theInputLock.Lock()
	theInput = testPluginInputCallbackDangle{}
	theInputLock.Unlock()

	cdone := make(chan bool)
	// #nosec G103 creating a NULL pointer should be fine.
	ptr := unsafe.Pointer(nil)

	// prepare channel for input explicitly.
	prepareInputCollector(false)

	go func() {
		t := time.NewTicker(collectInterval)
		defer t.Stop()

		FLBPluginInputCallback(&ptr, nil)
		for {
			select {
			case <-t.C:
				FLBPluginInputCallback(&ptr, nil)
			case <-cdone:
				return
			}
		}
	}()

	timeout := time.NewTimer(5 * time.Second)

	<-timeout.C
	timeout.Stop()
	runCancel()
	cdone <- true

	// Test the assumption that only a single goroutine is
	// ingesting records.
	if testPluginInputCallbackDangleFuncs.Load() != 1 {
		t.Fatalf("Too many callbacks: %d",
			testPluginInputCallbackDangleFuncs.Load())
	}
}

var testPluginInputCallbackInfiniteFuncs atomic.Int64

type testPluginInputCallbackInfinite struct{}

func (t testPluginInputCallbackInfinite) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t testPluginInputCallbackInfinite) Collect(ctx context.Context, ch chan<- Message) error {
	testPluginInputCallbackInfiniteFuncs.Add(1)
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
	theInputLock.Lock()
	theInput = testPluginInputCallbackInfinite{}
	theInputLock.Unlock()

	cdone := make(chan bool)
	cshutdown := make(chan bool)
	// #nosec G103 creating a NULL pointer should be fine.
	ptr := unsafe.Pointer(nil)

	// prepare channel for input explicitly.
	prepareInputCollector(false)

	go func() {
		t := time.NewTicker(collectInterval)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				FLBPluginInputCallback(&ptr, nil)
				if ptr != nil {
					cdone <- true
					return
				}
			case <-cshutdown:
				return
			}
		}
	}()

	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()

	select {
	case <-cdone:
		runCancel()
		// make sure Collect is not being invoked after Done().
		time.Sleep(collectInterval * 10)
		// Test the assumption that only a single goroutine is
		// ingesting records.
		if testPluginInputCallbackInfiniteFuncs.Load() != 1 {
			t.Fatalf("Too many callbacks: %d",
				testPluginInputCallbackInfiniteFuncs.Load())
		}
		return
	case <-timeout.C:
		runCancel()
		cshutdown <- true
		// This test seems to fail some what frequently because the Collect goroutine
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
			tick.Reset(time.Second * 1)
		case <-ctx.Done():
			return nil
		}
	}
}

// TestInputCallbackInfiniteLatency is a test of the latency between
// messages.
func TestInputCallbackLatency(t *testing.T) {
	theInputLock.Lock()
	theInput = testPluginInputCallbackLatency{}
	theInputLock.Unlock()

	cdone := make(chan bool)
	cstarted := make(chan bool)
	cmsg := make(chan []byte)

	// prepare channel for input explicitly.
	prepareInputCollector(false)

	go func() {
		t := time.NewTicker(collectInterval)
		defer t.Stop()

		buf, _ := testFLBPluginInputCallback()
		if len(buf) > 0 {
			cmsg <- buf
		}

		cstarted <- true
		for {
			select {
			case <-cdone:
				fmt.Println("---- collect done")
				return
			case <-t.C:
				buf, _ := testFLBPluginInputCallback()
				if len(buf) > 0 {
					cmsg <- buf
				}
			}
		}
	}()

	<-cstarted
	fmt.Println("---- started")
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

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
		case <-timeout.C:
			runCancel()
			cdone <- true

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
	theInputLock.Lock()
	theInput = testInputCallbackInfiniteConcurrent{}
	theInputLock.Unlock()

	cdone := make(chan bool)
	cstarted := make(chan bool)
	// #nosec G103 creating a NULL pointer should be fine.
	ptr := unsafe.Pointer(nil)

	concurrentWait.Add(64)

	// prepare channel for input explicitly.
	prepareInputCollector(false)

	go func(cstarted chan bool) {
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()

		FLBPluginInputCallback(&ptr, nil)
		cstarted <- true

		for {
			select {
			case <-ticker.C:
				FLBPluginInputCallback(&ptr, nil)
			case <-runCtx.Done():
				return
			}
		}
	}(cstarted)

	go func() {
		concurrentWait.Wait()
		cdone <- true
	}()

	<-cstarted
	timeout := time.NewTimer(10 * time.Second)

	select {
	case <-cdone:
		runCancel()
	case <-timeout.C:
		runCancel()
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

	out := testOutputHandlerReflect{
		Test: t,
		Check: func(t *testing.T, msg Message) error {
			defer wg.Done()

			assert.Equal(t, now, msg.Time)

			record := assertType[map[string]any](t, msg.Record)

			foo := assertType[string](t, record["foo"])
			assert.Equal(t, "bar", foo)

			bar := assertType[int8](t, record["bar"])
			assert.Equal(t, 3, bar)

			foobar := assertType[float64](t, record["foobar"])
			assert.Equal(t, 1.337, foobar)

			return nil
		},
	}
	_ = prepareOutputFlush(&out)

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
	assert.NoError(t, pluginFlush("foobar", b))
	wg.Wait()
}

func assertType[T any](tb testing.TB, got any) T {
	tb.Helper()

	var want T

	v, ok := got.(T)
	assert.True(tb, ok, "Expected types to be equal:\n-%T\n+%T", want, got)

	return v
}

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple camelCase", "skipOwnershipChallenge", "skip_ownership_challenge"},
		{"with uppercase acronym", "HTTPCode", "http_code"},
		{"mixed case acronym", "XMLHttpRequest", "xml_http_request"},
		{"already snake_case", "already_snake", "already_snake"},
		{"single word lowercase", "token", "token"},
		{"single word uppercase", "TOKEN", "token"},
		{"starts with uppercase", "ApiKey", "api_key"},
		{"multiple acronyms", "HTTPSConnectionURL", "https_connection_url"},
		{"acronym at end", "connectHTTPS", "connect_https"},
		{"numbers", "base64Encoded", "base64_encoded"},
		{"consecutive capitals", "IOError", "io_error"},
		{"camelCase with TLS", "calyptiaTLS", "calyptia_tls"},
		{"camelCase host", "calyptiaHost", "calyptia_host"},
		{"max buffered messages", "maxBufferedMessages", "max_buffered_messages"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toSnakeCase(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestNumberTypePreservation tests that the decodeMsg function preserves
// number types (int64 vs float64) instead of converting all numbers to float64.
func TestNumberTypePreservation(t *testing.T) {
	now := time.Now()

	// Create a message with various number types that should be preserved
	originalRecord := map[string]any{
		"integer_positive": int64(42),
		"integer_negative": int64(-123),
		"integer_zero":     int64(0),
		"integer_large":    int64(9223372036854775807), // max int64
		"float_simple":     float64(3.14),
		"float_negative":   float64(-2.71),
		"float_zero":       float64(0.0),
		"float_scientific": float64(1.23e-4),
		"string_value":     "test",
		"boolean_value":    true,
		"mixed_in_array":   []any{int64(1), float64(2.5), int64(3)},
		"nested_map": map[string]any{
			"nested_int":   int64(456),
			"nested_float": float64(7.89),
		},
	}

	msg := Message{
		Time:   now,
		Record: originalRecord,
	}

	// Marshal the message as it would be done by the input plugin
	marshaledData, err := msgpack.Marshal([]any{
		&EventTime{msg.Time},
		msg.Record,
	})
	assert.NoError(t, err)

	// Create a decoder and decode the message using our fixed decodeMsg function
	decoder := msgpack.NewDecoder(bytes.NewReader(marshaledData))
	decodedMsg, err := decodeMsg(decoder, "test-tag")
	assert.NoError(t, err)

	// Verify the decoded message structure
	if decodedMsg.Record == nil {
		t.Fatal("Record should not be nil")
	}

	record, ok := decodedMsg.Record.(map[string]any)
	if !ok {
		t.Fatal("Record should be a map[string]any")
	}

	// Compare each field from original record with decoded record
	// assert.Equal checks both value and type, so this ensures type preservation
	for k, v := range originalRecord {
		decodedValue := record[k]
		switch originalValue := v.(type) {
		case []any:
			decodedSlice := assertType[[]any](t, decodedValue)
			assert.Equal(t, len(originalValue), len(decodedSlice), "Array length for field %q", k)
			for i, originalItem := range originalValue {
				assert.Equal(t, originalItem, decodedSlice[i], "Field %q[%d]", k, i)
			}
		case map[string]any:
			decodedMap := assertType[map[string]any](t, decodedValue)
			for nestedKey, nestedOriginal := range originalValue {
				assert.Equal(t, nestedOriginal, decodedMap[nestedKey], "Field %q.%q", k, nestedKey)
			}
		default:
			assert.Equal(t, v, decodedValue, "Field %q", k)
		}
	}

	// Verify that tag is preserved
	assert.Equal(t, "test-tag", decodedMsg.Tag())
}
