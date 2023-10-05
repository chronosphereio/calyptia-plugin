package plugin

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/calyptia/plugin/output"
)

type testPluginInputCallbackCtrlC struct{}

func (t testPluginInputCallbackCtrlC) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t testPluginInputCallbackCtrlC) Collect(ctx context.Context, ch chan<- Message) error {
	return nil
}

func init() {
	initWG.Done()
	registerWG.Done()
}

func TestInputCallbackCtrlC(t *testing.T) {
	defer flbPluginReset()

	theInput = testPluginInputCallbackCtrlC{}
	cdone := make(chan bool)
	timeout := time.NewTimer(1 * time.Second)
	ptr := unsafe.Pointer(nil)

	go func() {
		FLBPluginInputCallback(&ptr, nil)
		cdone <- true
	}()

	select {
	case <-cdone:
		runCancel()
	case <-timeout.C:
		t.Fail()
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
	defer flbPluginReset()

	theInput = testPluginInputCallbackDangle{}
	cdone := make(chan bool)
	ptr := unsafe.Pointer(nil)

	go func() {
		t := time.NewTicker(collectInterval)
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

	// Test the assumption that only a single goroutine is
	// ingesting records.
	if testPluginInputCallbackDangleFuncs.Load() != 1 {
		fmt.Printf("Too many callbacks: %d",
			testPluginInputCallbackDangleFuncs.Load())
		t.Fail()
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
	defer flbPluginReset()

	theInput = testPluginInputCallbackInfinite{}
	cdone := make(chan bool)
	ptr := unsafe.Pointer(nil)

	go func() {
		for {
			FLBPluginInputCallback(&ptr, nil)
			time.Sleep(collectInterval)

			if ptr != nil {
				cdone <- true
			}
		}
	}()

	timeout := time.NewTimer(10 * time.Second)

	select {
	case <-cdone:
		timeout.Stop()
		runCancel()
		// make sure Collect is not being invoked after Done().
		time.Sleep(collectInterval * 10)
		// Test the assumption that only a single goroutine is
		// ingesting records.
		if testPluginInputCallbackInfiniteFuncs.Load() != 1 {
			fmt.Printf("Too many callbacks: %d",
				testPluginInputCallbackInfiniteFuncs.Load())
			t.Fail()
		}
		return
	case <-timeout.C:
		fmt.Println("---- Timed out....")
		runCancel()
		t.Fail()
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
	defer flbPluginReset()

	theInput = testPluginInputCallbackLatency{}
	cdone := make(chan bool)
	cmsg := make(chan []byte)

	go func() {
		t := time.NewTicker(collectInterval)
		for {
			select {
			case <-cdone:
				return
			case <-t.C:
				buf, _ := testFLBPluginInputCallback()
				if len(buf) > 0 {
					cmsg <- buf
				}
			}
		}
	}()

	timeout := time.NewTimer(5 * time.Second)
	msgs := 0

	for {
		select {
		case buf := <-cmsg:
			dec := output.NewByteDecoder(buf)
			if dec == nil {
				t.Fatal("dec is nil")
			}

			for {
				ret, timestamp, _ := output.GetRecord(dec)
				if ret == -1 {
					break
				}
				if ret < 0 {
					t.Fatalf("ret is negative: %d", ret)
				}

				msgs++

				ts, ok := timestamp.(output.FLBTime)
				if !ok {
					t.Fatal()
				}

				if time.Since(ts.Time) > time.Millisecond*5 {
					t.Errorf("latency too high: %fms",
						float64(time.Since(ts.Time)/time.Millisecond))
				}
			}
		case <-timeout.C:
			timeout.Stop()
			runCancel()

			if msgs < 128 {
				t.Fatalf("too few messages: %d", msgs)
			}
			return
		}
	}
}

type testInputCallbackInfiniteConcurrent struct{}

var concurrentWait sync.WaitGroup

func (t testInputCallbackInfiniteConcurrent) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t testInputCallbackInfiniteConcurrent) Collect(ctx context.Context, ch chan<- Message) error {
	defer flbPluginReset()

	for i := 0; i < 64; i++ {
		go func(ch chan<- Message, id int) {
			ch <- Message{
				Time: time.Now(),
				Record: map[string]string{
					"ID": fmt.Sprintf("%d", id),
				},
			}
			concurrentWait.Done()
		}(ch, i)
	}
	// for tests to correctly pass our infinite loop needs
	// to return once the context has been finished.
	for {
		select {
		case <-ctx.Done():
			return nil
		}
	}
}

// TestInputCallbackInfiniteConcurrent is meant to make sure we do not
// break anythin with respect to concurrent ingest.
func TestInputCallbackInfiniteConcurrent(t *testing.T) {
	defer flbPluginReset()

	theInput = testInputCallbackInfiniteConcurrent{}
	cdone := make(chan bool)
	timeout := time.NewTimer(10 * time.Second)
	ptr := unsafe.Pointer(nil)

	concurrentWait.Add(64)
	go func() {
		FLBPluginInputCallback(&ptr, nil)
		concurrentWait.Wait()
		cdone <- true
	}()

	select {
	case <-cdone:
		runCancel()
	case <-timeout.C:
		runCancel()
		t.Fail()
	}
}
