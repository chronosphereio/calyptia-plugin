package plugin

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/input"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/ugorji/go/codec"
)

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	defer initWG.Done()

	if theInput == nil || theOutput == nil {
		fmt.Fprintf(os.Stderr, "no input or output registered\n")
		return input.FLB_RETRY
	}

	if theInput != nil {
		return input.FLBPluginRegister(def, theName, theDesc)
	}

	return output.FLBPluginRegister(def, theName, theDesc)
}

//export FLBPluginInit
func FLBPluginInit(ptr unsafe.Pointer) int {
	defer setupWG.Done()

	if theInput == nil || theOutput == nil {
		fmt.Fprintf(os.Stderr, "no input or output registered\n")
		return input.FLB_RETRY
	}

	initWG.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf := &flbConfigLoader{ptr: ptr}

	var err error
	if theInput != nil {
		err = theInput.Setup(ctx, conf)
	} else {
		err = theOutput.Setup(ctx, conf)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "init: %v\n", err)
		return input.FLB_ERROR
	}

	return input.FLB_OK
}

//export FLBPluginInputCallback
func FLBPluginInputCallback(data *unsafe.Pointer, size *C.size_t) int {
	if theInput == nil {
		fmt.Fprintf(os.Stderr, "no input registered\n")
		return input.FLB_RETRY
	}

	setupWG.Wait()

	var err error
	once.Do(func() {
		runCtx, runCancel = context.WithCancel(context.Background())
		theChannel = make(chan Message, 1)
		go func() {
			defer runCancel()
			defer close(theChannel)
			err = theInput.Run(runCtx, theChannel)
		}()
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %s\n", err)
		return input.FLB_ERROR
	}

	select {
	case msg, ok := <-theChannel:
		if !ok {
			return input.FLB_OK
		}

		t := input.FLBTime{Time: msg.Time}
		b, err := input.NewEncoder().Encode([]any{t, msg.Record})
		if err != nil {
			fmt.Fprintf(os.Stderr, "encode: %s\n", err)
			return input.FLB_ERROR
		}

		*data = C.CBytes(b)
		*size = C.size_t(len(b))
	case <-runCtx.Done():
		err := runCtx.Err()
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "run: %s\n", err)
			return input.FLB_ERROR
		}
	}

	return input.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, clength C.int, ctag *C.char) int {
	if theOutput == nil {
		fmt.Fprintf(os.Stderr, "no output registered\n")
		return input.FLB_RETRY
	}

	setupWG.Wait()

	var err error
	once.Do(func() {
		runCtx, runCancel = context.WithCancel(context.Background())
		theChannel = make(chan Message, 1)
		go func() {
			defer runCancel()
			defer close(theChannel)

			tag := C.GoString(ctag)
			err = theOutput.Run(runCtx, tag, theChannel)
		}()
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %s\n", err)
		return input.FLB_ERROR
	}

	in := C.GoBytes(data, C.int(clength))
	h := &codec.MsgpackHandle{}
	h.SetBytesExt(reflect.TypeOf(fTime{}), 0, &fTime{})
	dec := codec.NewDecoderBytes(in, h)

	for {
		var entry []any
		if err := dec.Decode(&entry); err != nil {
			fmt.Fprintf(os.Stderr, "decode: %s\n", err)
			return input.FLB_ERROR
		}

		if d := len(entry); d != 2 {
			fmt.Fprintf(os.Stderr, "unexpected entry length: %d\n", d)
			return input.FLB_ERROR
		}

		ft, ok := entry[0].(fTime)
		if !ok {
			fmt.Fprintf(os.Stderr, "unexpected entry time type: %T\n", entry[0])
			return input.FLB_ERROR
		}

		t := time.Time(ft)

		rec, ok := entry[1].(map[string]string)
		if !ok {
			fmt.Fprintf(os.Stderr, "unexpected entry record type: %T\n", entry[1])
			return input.FLB_ERROR
		}

		theChannel <- Message{Time: t, Record: rec}
	}
}

//export FLBPluginExit
func FLBPluginExit() int {
	if runCancel != nil {
		runCancel()
	}

	defer close(theChannel)
	return input.FLB_OK
}

type flbConfigLoader struct {
	ptr unsafe.Pointer
}

func (f *flbConfigLoader) Load(key string) string {
	return input.FLBPluginConfigKey(f.ptr, key)
}
