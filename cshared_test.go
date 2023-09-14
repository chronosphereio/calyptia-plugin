package plugin

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
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
	theInput = testPluginInputCallbackInfinite{}
	cdone := make(chan bool)
	timeout := time.NewTimer(10 * time.Second)
	ptr := unsafe.Pointer(nil)

	go func() {
		for {
			FLBPluginInputCallback(&ptr, nil)
			time.Sleep(1 * time.Second)

			if ptr != nil {
				cdone <- true
			}
		}
	}()

	select {
	case <-cdone:
		runCancel()
		// Test the assumption that only a single goroutine is
		// ingesting records.
		if testPluginInputCallbackInfiniteFuncs.Load() != 1 {
			t.Fail()
		}
		return
	case <-timeout.C:
		runCancel()
		t.Fail()
	}
	t.Fail()
}

type testInputCallbackInfiniteConcurrent struct{}

var concurrentWait sync.WaitGroup

func (t testInputCallbackInfiniteConcurrent) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t testInputCallbackInfiniteConcurrent) Collect(ctx context.Context, ch chan<- Message) error {
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
