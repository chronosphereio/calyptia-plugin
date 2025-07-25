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
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5"

	cmetrics "github.com/calyptia/cmetrics-go"
	"github.com/calyptia/plugin/custom"
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

// FLBPluginPreRegister -
//
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

	if theInput == nil && theOutput == nil && theCustom == nil {
		fmt.Fprintf(os.Stderr, "no input or output or custom registered\n")
		return input.FLB_RETRY
	}

	if theInput != nil {
		out := input.FLBPluginRegister(def, theName, theDesc)
		unregister = func() {
			input.FLBPluginUnregister(def)
		}
		return out
	}

	if theOutput != nil {
		out := output.FLBPluginRegister(def, theName, theDesc)
		unregister = func() {
			output.FLBPluginUnregister(def)
		}
		return out
	}

	if theCustom != nil {
		out := custom.FLBPluginRegister(def, theName, theDesc)
		unregister = func() {
			custom.FLBPluginUnregister(def)
		}

		return out
	}

	return input.FLB_ERROR
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

	if theInput == nil && theOutput == nil && theCustom == nil {
		fmt.Fprintf(os.Stderr, "no input, output, or custom registered\n")
		return input.FLB_RETRY
	}

	ctx, cancel := context.WithCancel(context.Background())

	var err error
	switch {
	case theInput != nil:
		defer cancel()
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
			if maxbuffered, errbuf := strconv.Atoi(maxbuffered); errbuf != nil {
				maxBufferedMessages = maxbuffered
			}
		}
	case theOutput != nil:
		defer cancel()
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
	default:
		runCancel = cancel
		conf := &flbCustomConfigLoader{ptr: ptr}
		logger = &flbCustomLogger{ptr: ptr}
		fbit := &Fluentbit{
			Conf:    conf,
			Metrics: nil,
			Logger:  logger,
		}
		err = theCustom.Init(ctx, fbit)
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
	defer func() {
		if ret := recover(); ret != nil {
			fmt.Fprintf(os.Stderr, "Channel is already closed")
			return
		}
	}()

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

// prepareOutputFlush is a testing utility.
func prepareOutputFlush(output OutputPlugin) error {
	theOutput = output
	FLBPluginOutputPreRun(0)
	return nil
}

// Lock used to synchronize access to theInput variable.
var theInputLock sync.Mutex

// prepareInputCollector is meant to prepare resources for input collectors.
func prepareInputCollector(multiInstance bool) {
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
			err := theInput.Collect(runCtx, theChannel)
			if err != nil {
				fmt.Fprintf(os.Stderr, "collect error: %v\n", err)
			}
		}(theChannel)

		<-runCtx.Done()

		log.Printf("goroutine will be stopping: name=%q\n", theName)
	}(theChannel)
}

// FLBPluginInputPreRun this method gets invoked by the fluent-bit runtime, once the plugin has been
// initialized, the plugin invoked only once before executing the input callbacks.
//
//export FLBPluginInputPreRun
func FLBPluginInputPreRun(useHotReload C.int) int {
	registerWG.Wait()

	prepareInputCollector(true)

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
	prepareInputCollector(true)
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

// FLBPluginOutputPreRun -
//
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

		<-runCtx.Done()

		log.Printf("goroutine will be stopping: name=%q\n", theName)
	}(runCtx)

	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %s\n", err)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

// FLBPluginInputCallback this method gets invoked by the fluent-bit runtime, once the plugin has been
// initialized, the plugin implementation is responsible for handling the incoming data and the context
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

			b, err := msgpack.Marshal([]any{&EventTime{msg.Time}, msg.Record})
			if err != nil {
				fmt.Fprintf(os.Stderr, "msgpack marshal: %s\n", err)
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
	tag := C.GoString(ctag)
	if err := pluginFlush(tag, in); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %s\n", err)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

func pluginFlush(tag string, b []byte) error {
	dec := msgpack.NewDecoder(bytes.NewReader(b))
	for {
		select {
		case <-runCtx.Done():
			err := runCtx.Err()
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(os.Stderr, "run: %s\n", err)
				return fmt.Errorf("run: %w", err)
			}

			return nil
		default:
		}

		msg, err := decodeMsg(dec, tag)
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		theChannel <- msg
	}
}

// decodeMsg should be called with an already initialized decoder.
func decodeMsg(dec *msgpack.Decoder, tag string) (Message, error) {
	var out Message

	var entry []msgpack.RawMessage
	err := dec.Decode(&entry)
	if errors.Is(err, io.EOF) {
		return out, err
	}

	if err != nil {
		return out, fmt.Errorf("msgpack unmarshal: %w", err)
	}

	if l := len(entry); l < 2 {
		return out, fmt.Errorf("msgpack unmarshal: expected 2 elements, got %d", l)
	}

	eventTime := &EventTime{}
	if err := msgpack.Unmarshal(entry[0], &eventTime); err != nil {
		var eventWithMetadata []msgpack.RawMessage // for Fluent Bit V2 metadata type of format
		if err = msgpack.Unmarshal(entry[0], &eventWithMetadata); err != nil {
			return out, fmt.Errorf("msgpack unmarshal event with metadata: %w", errmsgpack)
		}

		if len(eventWithMetadata) < 1 {
			return out, fmt.Errorf("msgpack unmarshal event time with metadata: expected 1 element, got %d", len(eventWithMetadata))
		}

		if errunpack := msgpack.Unmarshal(eventWithMetadata[0], &eventTime); errunpack != nil {
			return out, fmt.Errorf("msgpack unmarshal event time with metadata: %w", errunpack)
		}

		return out, fmt.Errorf("msgpack unmarshal event time: %w", err)
	}

	var record map[string]any
	if err := msgpack.Unmarshal(entry[1], &record); err != nil {
		return out, fmt.Errorf("msgpack unmarshal event record: %w", err)
	}

	out.Time = eventTime.UTC()
	out.Record = record
	out.tag = &tag

	return out, nil
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

// String retrieves a configuration value by key.
// All configuration keys in Fluent Bit are in snake_case format.
// This method automatically converts camelCase keys to snake_case,
// so both "apiKey" and "api_key" will retrieve the same value.
func (f *flbInputConfigLoader) String(key string) string {
	return unquote(input.FLBPluginConfigKey(f.ptr, toSnakeCase(key)))
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

// String retrieves a configuration value by key.
// All configuration keys in Fluent Bit are in snake_case format.
// This method automatically converts camelCase keys to snake_case,
// so both "apiKey" and "api_key" will retrieve the same value.
func (f *flbOutputConfigLoader) String(key string) string {
	return unquote(output.FLBPluginConfigKey(f.ptr, toSnakeCase(key)))
}

type flbCustomConfigLoader struct {
	ptr unsafe.Pointer
}

// String retrieves a configuration value by key.
// All configuration keys in Fluent Bit are in snake_case format.
// This method automatically converts camelCase keys to snake_case,
// so both "apiKey" and "api_key" will retrieve the same value.
func (f *flbCustomConfigLoader) String(key string) string {
	return unquote(custom.FLBPluginConfigKey(f.ptr, toSnakeCase(key)))
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

type flbCustomLogger struct {
	ptr unsafe.Pointer
}

func (f *flbCustomLogger) Error(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	custom.FLBPluginLogPrint(f.ptr, output.FLB_LOG_ERROR, message)
}

func (f *flbCustomLogger) Warn(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	custom.FLBPluginLogPrint(f.ptr, output.FLB_LOG_WARN, message)
}

func (f *flbCustomLogger) Info(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	custom.FLBPluginLogPrint(f.ptr, output.FLB_LOG_INFO, message)
}

func (f *flbCustomLogger) Debug(format string, a ...any) {
	message := fmt.Sprintf(format, a...)
	custom.FLBPluginLogPrint(f.ptr, output.FLB_LOG_DEBUG, message)
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

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

// toSnakeCase converts a camelCase string to snake_case.
// Taken from https://stackoverflow.com/questions/56616196/how-to-convert-camel-case-string-to-snake-case.
func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
