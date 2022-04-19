package plugin

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"errors"
	"fmt"
	"io"
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

	if theInput == nil && theOutput == nil {
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

	if theInput == nil && theOutput == nil {
		fmt.Fprintf(os.Stderr, "no input or output registered\n")
		return input.FLB_RETRY
	}

	initWG.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf := &flbConfigLoader{ptr: ptr}

	var err error
	if theInput != nil {
		conf.kind = "input"
		err = theInput.Init(ctx, conf)
	} else {
		conf.kind = "output"
		err = theOutput.Init(ctx, conf)
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
			err = theInput.Collect(runCtx, theChannel)
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
		return output.FLB_RETRY
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
			err = theOutput.Collect(runCtx, tag, theChannel)
		}()
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %s\n", err)
		return output.FLB_ERROR
	}

	in := C.GoBytes(data, C.int(clength))
	h := &codec.MsgpackHandle{}
	h.SetBytesExt(reflect.TypeOf(fTime{}), 0, &fTime{})
	dec := codec.NewDecoderBytes(in, h)

	for {
		var entry []any
		err := dec.Decode(&entry)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "decode: %s\n", err)
			return output.FLB_ERROR
		}

		if d := len(entry); d != 2 {
			fmt.Fprintf(os.Stderr, "unexpected entry length: %d\n", d)
			return output.FLB_ERROR
		}

		ft, ok := entry[0].(fTime)
		if !ok {
			fmt.Fprintf(os.Stderr, "unexpected entry time type: %T\n", entry[0])
			return output.FLB_ERROR
		}

		t := time.Time(ft)

		recVal, ok := entry[1].(map[any]any)
		if !ok {
			fmt.Fprintf(os.Stderr, "unexpected entry record type: %T\n", entry[1])
			return output.FLB_ERROR
		}

		var rec map[string]string
		if d := len(recVal); d != 0 {
			rec = make(map[string]string, d)
			for k, v := range recVal {
				key, ok := k.(string)
				if !ok {
					fmt.Fprintf(os.Stderr, "unexpected record key type: %T\n", k)
					return output.FLB_ERROR
				}

				val, ok := v.([]uint8)
				if !ok {
					fmt.Fprintf(os.Stderr, "unexpected record value type: %T\n", v)
					return output.FLB_ERROR
				}

				rec[string(key)] = string(val)
			}
		}

		theChannel <- Message{Time: t, Record: rec}
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	if runCancel != nil {
		runCancel()
	}

	if theChannel != nil {
		defer close(theChannel)
	}

	return input.FLB_OK
}

type flbConfigLoader struct {
	ptr  unsafe.Pointer
	kind string
}

func (f *flbConfigLoader) String(key string) string {
	switch f.kind {
	case "input":
		return input.FLBPluginConfigKey(f.ptr, key)
	case "output":
		return output.FLBPluginConfigKey(f.ptr, key)
	}
	return ""
}
