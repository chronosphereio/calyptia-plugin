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

	ptr := unsafe.Pointer(nil)

	// prepare channel for input explicitly.
	err := prepareInputCollector(false)
	if err != nil {
		t.Fail()
		return
	}

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
	ptr := unsafe.Pointer(nil)

	// prepare channel for input explicitly.
	err := prepareInputCollector(false)
	if err != nil {
		t.Fail()
	}

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
	ptr := unsafe.Pointer(nil)

	// prepare channel for input explicitly.
	err := prepareInputCollector(false)
	if err != nil {
		t.Fail()
		return
	}

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
	err := prepareInputCollector(false)
	if err != nil {
		t.Fail()
		return
	}

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

var concurrentWait sync.WaitGroup
var concurrentCountStart atomic.Int64
var concurrentCountFinish atomic.Int64

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
	theInputLock.Lock()
	theInput = testInputCallbackInfiniteConcurrent{}
	theInputLock.Unlock()

	cdone := make(chan bool)
	cstarted := make(chan bool)
	ptr := unsafe.Pointer(nil)

	concurrentWait.Add(64)

	// prepare channel for input explicitly.
	err := prepareInputCollector(false)
	if err != nil {
		t.Fail()
	}

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
