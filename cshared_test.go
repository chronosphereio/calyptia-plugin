package plugin

import (
	"context"
	"testing"
	"time"
	"unsafe"
)

type testPluginInputCallback struct{}

func (t testPluginInputCallback) Init(ctx context.Context, fbit *Fluentbit) error {
	return nil
}

func (t testPluginInputCallback) Collect(ctx context.Context, send func(t time.Time, record map[string]any)) error {
	return nil
}

func TestInputCallbackCtrlC(t *testing.T) {
	theInput = testPluginInputCallback{}
	cdone := make(chan bool)
	timeout := time.NewTimer(1 * time.Second)
	ptr := unsafe.Pointer(nil)

	initWG.Done()
	registerWG.Done()

	go func() {
		FLBPluginInputCallback(&ptr, nil)
		cdone <- true
	}()

	select {
	case <-cdone:
	case <-timeout.C:
		t.Fail()
	}
}
