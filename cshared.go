package plugin

/*
#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/ugorji/go/codec"

	cmetrics "github.com/calyptia/cmetrics-go"
	"github.com/calyptia/plugin/input"
	metricbuilder "github.com/calyptia/plugin/metric/cmetric"
	"github.com/calyptia/plugin/output"
)

const (
	// maxBufferedMessages is the number of messages that will be buffered
	// between each fluent-bit interval (approx 1 second).
	defaultMaxBufferedMessages = 300000
	// collectInterval is set to the interval present before in core-fluent-bit.
	collectInterval = 1000 * time.Nanosecond
)

var (
	unregister          func()
	cmt                 *cmetrics.Context
	logger              Logger
	maxBufferedMessages = defaultMaxBufferedMessages
)

//export FLBPluginPreRegister
func FLBPluginPreRegister(hotReloading C.int) int {
	if hotReloading == C.int(1) {
		registerWG.Add(1)
	}

	return input.FLB_OK
}

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

func cleanup() int {
	if unregister != nil {
		unregister()
		unregister = nil
	}

	if runCancel != nil {
		runCancel()
		runCancel = nil
	}

	if !theInputLock.TryLock() {
		return input.FLB_OK
	}
	defer theInputLock.Unlock()

	if theChannel != nil {
		defer close(theChannel)
	}

	return input.FLB_OK
}

// FLBPluginInit this method gets invoked once by the fluent-bit runtime at initialisation phase.
// here all the plugin context should be initialized and any data or flag required for
// plugins to execute the collect or flush callback.
//
//export FLBPluginInit
func FLBPluginInit(ptr unsafe.Pointer) int {
	initWG.Add(1)
	defer initWG.Done()

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
		if maxbuffered := fbit.Conf.String("go.MaxBufferedMessages"); maxbuffered != "" {
			maxbuffered, err := strconv.Atoi(maxbuffered)
			if err != nil {
				maxBufferedMessages = maxbuffered
			}
		}
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

// flbPluginReset is meant to reset the plugin between tests.
func flbPluginReset() {
	theInputLock.Lock()
	defer theInputLock.Unlock()

	once = sync.Once{}
	close(theChannel)
	theInput = nil
}

func testFLBPluginInputCallback() ([]byte, error) {
	data := unsafe.Pointer(nil)
	var csize C.size_t

	FLBPluginInputCallback(&data, &csize)

	if data == nil {
		return []byte{}, nil
	}

	defer C.free(data)
	return C.GoBytes(data, C.int(csize)), nil
}

// Lock used to synchronize access to theInput variable.
var theInputLock sync.Mutex

// prepareInputCollector is meant to prepare resources for input collectors
func prepareInputCollector(multiInstance bool) (err error) {
	runCtx, runCancel = context.WithCancel(context.Background())
	if !multiInstance {
		theChannel = make(chan Message, maxBufferedMessages)
	}

	theInputLock.Lock()
	if multiInstance {
		if theChannel == nil {
			theChannel = make(chan Message, maxBufferedMessages)
		}
		defer theInputLock.Unlock()
	}

	go func(theChannel chan<- Message) {
		if !multiInstance {
			defer theInputLock.Unlock()
		}

		go func(theChannel chan<- Message) {
			err = theInput.Collect(runCtx, theChannel)
		}(theChannel)

		for {
			select {
			case <-runCtx.Done():
				log.Printf("goroutine will be stopping: name=%q\n", theName)
				return
			}
		}

		if err != nil {
			fmt.Fprintf(os.Stderr,
				"collect error: %s\n", err.Error())
		}
	}(theChannel)

	return err
}

// FLBPluginInputPreRun this method gets invoked by the fluent-bit runtime, once the plugin has been
// initialised, the plugin invoked only once before executing the input callbacks.
//
//export FLBPluginInputPreRun
func FLBPluginInputPreRun(useHotReload C.int) int {
	registerWG.Wait()

	var err error
	err = prepareInputCollector(true)

	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %s\n", err)
		return input.FLB_ERROR
	}

	return input.FLB_OK
}

// FLBPluginInputPause this method gets invoked by the fluent-bit runtime, once the plugin has been
// paused, the plugin invoked this method and entering paused state.
//
//export FLBPluginInputPause
func FLBPluginInputPause() {
	if runCancel != nil {
		runCancel()
		runCancel = nil
	}

	if !theInputLock.TryLock() {
		return
	}
	defer theInputLock.Unlock()

	if theChannel != nil {
		close(theChannel)
		theChannel = nil
	}
}

// FLBPluginInputResume this method gets invoked by the fluent-bit runtime, once the plugin has been
// resumeed, the plugin invoked this method and re-running state.
//
//export FLBPluginInputResume
func FLBPluginInputResume() {
	var err error
	err = prepareInputCollector(true)

	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %s\n", err)
	}
}

// FLBPluginOutputPreExit this method gets invoked by the fluent-bit runtime, once the plugin has been
// exited, the plugin invoked this method and entering exiting state.
//
//export FLBPluginOutputPreExit
func FLBPluginOutputPreExit() {
	if runCancel != nil {
		runCancel()
		runCancel = nil
	}

	if !theInputLock.TryLock() {
		return
	}
	defer theInputLock.Unlock()

	if theChannel != nil {
		close(theChannel)
		theChannel = nil
	}
}

//export FLBPluginOutputPreRun
func FLBPluginOutputPreRun(useHotReload C.int) int {
	registerWG.Wait()

	var err error
	runCtx, runCancel = context.WithCancel(context.Background())
	theChannel = make(chan Message)
	go func(runCtx context.Context) {
		go func(runCtx context.Context) {
			err = theOutput.Flush(runCtx, theChannel)
		}(runCtx)

		for {
			select {
			case <-runCtx.Done():
				log.Printf("goroutine will be stopping: name=%q\n", theName)
				return
			}
		}

	}(runCtx)

	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %s\n", err)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

// FLBPluginInputCallback this method gets invoked by the fluent-bit runtime, once the plugin has been
// initialised, the plugin implementation is responsible for handling the incoming data and the context
// that gets past, for long-living collectors the plugin itself should keep a running thread and fluent-bit
// will not execute further callbacks.
//
// This function will invoke Collect only once to preserve backward
// compatible behavior. There are unit tests to enforce this behavior.
//
//export FLBPluginInputCallback
func FLBPluginInputCallback(data *unsafe.Pointer, csize *C.size_t) int {
	initWG.Wait()

	if theInput == nil {
		fmt.Fprintf(os.Stderr, "no input registered\n")
		return input.FLB_RETRY
	}

	buf := bytes.NewBuffer([]byte{})

	for loop := min(len(theChannel), maxBufferedMessages); loop > 0; loop-- {
		select {
		case msg, ok := <-theChannel:
			if !ok {
				return input.FLB_ERROR
			}

			t := input.FLBTime{Time: msg.Time}
			b, err := input.NewEncoder().Encode([]any{t, msg.Record})
			if err != nil {
				fmt.Fprintf(os.Stderr, "encode: %s\n", err)
				return input.FLB_ERROR
			}
			buf.Grow(len(b))
			buf.Write(b)
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
			loop = 0
		default:
			loop = 0
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
//nolint:funlen //ignore length requirement for this function
func FLBPluginFlush(data unsafe.Pointer, clength C.int, ctag *C.char) int {
	initWG.Wait()

	if theOutput == nil {
		fmt.Fprintf(os.Stderr, "no output registered\n")
		return output.FLB_RETRY
	}

	var err error
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

		slice := reflect.ValueOf(entry)
		if slice.Kind() != reflect.Slice || slice.Len() < 2 {
			fmt.Fprintf(os.Stderr, "unexpected entry length: %d\n", slice.Len())
			return output.FLB_ERROR
		}

		var t time.Time
		ts := slice.Index(0).Interface()
		switch ft := ts.(type) {
		case bigEndianTime:
			t = time.Time(ft)
		case []interface{}:
			s := reflect.ValueOf(ft)
			st := s.Index(0).Interface()
			ty := st.(bigEndianTime)
			t = time.Time(ty)
		default:
			fmt.Fprintf(os.Stderr, "unexpected entry time type: %T\n", entry[0])
			return output.FLB_ERROR
		}

		data := slice.Index(1)
		recVal := data.Interface().(map[interface{}]interface{})

		var rec map[string]string
		if d := len(recVal); d != 0 {
			rec = make(map[string]string, d)
			for k, v := range recVal {
				key, ok := k.(string)
				if !ok {
					fmt.Fprintf(os.Stderr, "unexpected record key type: %T\n", k)
					return output.FLB_ERROR
				}

				var val string
				switch tv := v.(type) {
				case []uint8:
					val = string(tv)
				case uint64:
					val = strconv.FormatUint(tv, 10)
				case int64:
					val = strconv.FormatInt(tv, 10)
				case bool:
					val = strconv.FormatBool(tv)
				default:
					fmt.Fprintf(os.Stderr, "unexpected record value type: %T\n", v)
					return output.FLB_ERROR
				}

				rec[key] = val
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
	return cleanup()
}

type flbInputConfigLoader struct {
	ptr unsafe.Pointer
}

func (f *flbInputConfigLoader) String(key string) string {
	return unquote(input.FLBPluginConfigKey(f.ptr, key))
}

func unquote(s string) string {
	if tmp, err := strconv.Unquote(s); err == nil {
		return tmp
	}

	// unescape literal newlines
	if strings.Contains(s, `\n`) {
		if tmp2, err := strconv.Unquote(`"` + s + `"`); err == nil {
			return tmp2
		}
	}

	return s
}

type flbOutputConfigLoader struct {
	ptr unsafe.Pointer
}

func (f *flbOutputConfigLoader) String(key string) string {
	return unquote(output.FLBPluginConfigKey(f.ptr, key))
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
