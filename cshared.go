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
	"log"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"time"
	"unsafe"

	"github.com/ugorji/go/codec"

	cmetrics "github.com/calyptia/cmetrics-go"
	"github.com/calyptia/plugin/input"
	metricbuilder "github.com/calyptia/plugin/metric/cmetric"
	"github.com/calyptia/plugin/output"
)

var unregister func()
var cmt *cmetrics.Context
var logger Logger

// FLBPluginRegister registers a plugin in the context of the fluent-bit runtime, a name and description
// can be provided.
//
//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	defer registerWG.Done()

	if theInput == nil && theOutput == nil {
		fmt.Fprintf(os.Stderr, "no input or output registered\n")
		return input.FLB_RETRY
	}

	if theInput != nil {
		out := input.FLBPluginRegister(def, theName, theDesc)
		unregister = func() {
			input.FLBPluginUnregister(def)
		}
		return out
	}

	out := output.FLBPluginRegister(def, theName, theDesc)
	unregister = func() {
		output.FLBPluginUnregister(def)
	}

	return out
}

// FLBPluginInit this method gets invoked once by the fluent-bit runtime at initialisation phase.
// here all the plugin context should be initialised and any data or flag required for
// plugins to execute the collect or flush callback.
//
//export FLBPluginInit
func FLBPluginInit(ptr unsafe.Pointer) int {
	defer initWG.Done()

	registerWG.Wait()

	if theInput == nil && theOutput == nil {
		fmt.Fprintf(os.Stderr, "no input or output registered\n")
		return input.FLB_RETRY
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var err error
	if theInput != nil {
		conf := &flbInputConfigLoader{ptr: ptr}
		cmt, err = input.FLBPluginGetCMetricsContext(ptr)
		if err != nil {
			return input.FLB_ERROR
		}
		logger = &flbInputLogger{ptr: ptr}
		fbit := &Fluentbit{
			Conf:    conf,
			Metrics: makeMetrics(cmt),
			Logger:  logger,
		}

		err = theInput.Init(ctx, fbit)
	} else {
		conf := &flbOutputConfigLoader{ptr: ptr}
		cmt, err = output.FLBPluginGetCMetricsContext(ptr)
		if err != nil {
			return output.FLB_ERROR
		}
		logger = &flbOutputLogger{ptr: ptr}
		fbit := &Fluentbit{
			Conf:    conf,
			Metrics: makeMetrics(cmt),
			Logger:  logger,
		}
		err = theOutput.Init(ctx, fbit)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "init: %v\n", err)
		return input.FLB_ERROR
	}

	return input.FLB_OK
}

// FLBPluginInputCallback this method gets invoked by the fluent-bit runtime, once the plugin has been
// initialised, the plugin implementation is responsible for handling the incoming data and the context
// that gets past, for long-living collectors the plugin itself should keep a running thread and fluent-bit
// will not execute further callbacks.
//
//export FLBPluginInputCallback
func FLBPluginInputCallback(data *unsafe.Pointer, csize *C.size_t) int {
	initWG.Wait()

	if theInput == nil {
		fmt.Fprintf(os.Stderr, "no input registered\n")
		return input.FLB_RETRY
	}

	var err error
	once.Do(func() {
		runCtx, runCancel = context.WithCancel(context.Background())
		theChannel = make(chan Message)
		go func() {
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

		cdata := C.CBytes(b)

		*data = cdata
		*csize = C.size_t(len(b))

		// C.free(unsafe.Pointer(cdata))
	case <-runCtx.Done():
		err := runCtx.Err()
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "run: %s\n", err)
			return input.FLB_ERROR
		}
		// enforce a runtime gc, to prevent the thread finalizer on
		// fluent-bit to kick in before any remaining data has not been GC'ed
		// causing a sigsegv.
		defer runtime.GC()
	}

	return input.FLB_OK
}

// FLBPluginInputCleanupCallback releases the memory used during the input callback
//
//export FLBPluginInputCleanupCallback
func FLBPluginInputCleanupCallback(data unsafe.Pointer) int {
	C.free(data)
	return input.FLB_OK
}

// FLBPluginFlush callback gets invoked by the fluent-bit runtime once there is data for the corresponding
// plugin in the pipeline, a data pointer, length and a tag are passed to the plugin interface implementation.
//
//export FLBPluginFlush
//nolint:funlen,gocognit,gocyclo //ignore length requirement for this function, TODO: refactor into smaller functions.
func FLBPluginFlush(data unsafe.Pointer, clength C.int, ctag *C.char) int {
	initWG.Wait()

	if theOutput == nil {
		fmt.Fprintf(os.Stderr, "no output registered\n")
		return output.FLB_RETRY
	}

	var err error
	once.Do(func() {
		runCtx, runCancel = context.WithCancel(context.Background())
		theChannel = make(chan Message)
		go func() {
			err = theOutput.Flush(runCtx, theChannel)
		}()
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %s\n", err)
		return output.FLB_ERROR
	}

	select {
	case <-runCtx.Done():
		err = runCtx.Err()
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "run: %s\n", err)
			return output.FLB_ERROR
		}

		return output.FLB_OK
	default:
	}

	in := C.GoBytes(data, clength)
	h := &codec.MsgpackHandle{}
	err = h.SetBytesExt(reflect.TypeOf(bigEndianTime{}), 0, &bigEndianTime{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "big endian time bytes ext: %v\n", err)
		return output.FLB_ERROR
	}

	dec := codec.NewDecoderBytes(in, h)

	for {
		select {
		case <-runCtx.Done():
			err := runCtx.Err()
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(os.Stderr, "run: %s\n", err)
				return output.FLB_ERROR
			}

			return output.FLB_OK
		default:
		}

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

		ft, ok := entry[0].(bigEndianTime)
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

				rec[key] = string(val)
			}
		}

		tag := C.GoString(ctag)
		// C.free(unsafe.Pointer(ctag))

		theChannel <- Message{Time: t, Record: rec, tag: &tag}

		// C.free(data)
		// C.free(unsafe.Pointer(&clength))
	}

	return output.FLB_OK
}

// FLBPluginExit method is invoked once the plugin instance is exited from the fluent-bit context.
//
//export FLBPluginExit
func FLBPluginExit() int {
	log.Printf("calling FLBPluginExit(): name=%q\n", theName)

	if unregister != nil {
		unregister()
	}

	if runCancel != nil {
		runCancel()
	}

	if theChannel != nil {
		defer close(theChannel)
	}

	return input.FLB_OK
}

type flbInputConfigLoader struct {
	ptr unsafe.Pointer
}

func (f *flbInputConfigLoader) String(key string) string {
	s := input.FLBPluginConfigKey(f.ptr, key)
	if tmp, err := strconv.Unquote(s); err == nil {
		return tmp
	}
	return s
}

type flbOutputConfigLoader struct {
	ptr unsafe.Pointer
}

func (f *flbOutputConfigLoader) String(key string) string {
	s := output.FLBPluginConfigKey(f.ptr, key)
	if tmp, err := strconv.Unquote(s); err == nil {
		return tmp
	}
	return s
}

type flbInputLogger struct {
	ptr unsafe.Pointer
}

func (f *flbInputLogger) Error(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	input.FLBPluginLogPrint(f.ptr, input.FLB_LOG_ERROR, message)
}

func (f *flbInputLogger) Warn(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	input.FLBPluginLogPrint(f.ptr, input.FLB_LOG_WARN, message)
}

func (f *flbInputLogger) Info(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	input.FLBPluginLogPrint(f.ptr, input.FLB_LOG_INFO, message)
}

func (f *flbInputLogger) Debug(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	input.FLBPluginLogPrint(f.ptr, input.FLB_LOG_DEBUG, message)
}

type flbOutputLogger struct {
	ptr unsafe.Pointer
}

func (f *flbOutputLogger) Error(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	output.FLBPluginLogPrint(f.ptr, output.FLB_LOG_ERROR, message)
}

func (f *flbOutputLogger) Warn(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	output.FLBPluginLogPrint(f.ptr, output.FLB_LOG_WARN, message)
}

func (f *flbOutputLogger) Info(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	output.FLBPluginLogPrint(f.ptr, output.FLB_LOG_INFO, message)
}

func (f *flbOutputLogger) Debug(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	output.FLBPluginLogPrint(f.ptr, output.FLB_LOG_DEBUG, message)
}

func makeMetrics(cmp *cmetrics.Context) Metrics {
	return &metricbuilder.Builder{
		Namespace: "fluentbit",
		SubSystem: "plugin",
		Context:   cmp,
		OnError: func(err error) {
			fmt.Fprintf(os.Stderr, "metrics: %s\n", err)
		},
	}
}
