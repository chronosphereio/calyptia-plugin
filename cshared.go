package plugin

/*
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
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
	meta := pluginMeta.Load()
	if meta == nil {
		fmt.Fprintf(os.Stderr, "no input, output, or custom plugin registered\n")
		return input.FLB_RETRY
	}

	var newUnregisterFunc func()
	var registerCode int
	if meta.input != nil {
		registerCode = input.FLBPluginRegister(def, meta.name, meta.desc)
		newUnregisterFunc = func() { input.FLBPluginUnregister(def) }
	} else if meta.output != nil {
		registerCode = output.FLBPluginRegister(def, meta.name, meta.desc)
		newUnregisterFunc = func() { output.FLBPluginUnregister(def) }
	} else if meta.custom != nil {
		registerCode = custom.FLBPluginRegister(def, meta.name, meta.desc)
		newUnregisterFunc = func() { custom.FLBPluginUnregister(def) }
	} else {
		fmt.Fprintf(os.Stderr, "no input, output, or custom plugin registered\n")
		return input.FLB_RETRY
	}

	if registerCode != 0 {
		fmt.Fprintf(os.Stderr, "error calling plugin register function\n")
		return input.FLB_RETRY
	}

	unregisterFunc.Store(&newUnregisterFunc)
	registerWG.Done()

	return input.FLB_OK
}

// FLBPluginInit this method gets invoked once by the fluent-bit runtime at initialisation phase.
// here all the plugin context should be initialized and any data or flag required for
// plugins to execute the collect or flush callback.
//
//export FLBPluginInit
func FLBPluginInit(ptr unsafe.Pointer) (respCode int) {
	currInstanceMu.Lock()
	defer currInstanceMu.Unlock()

	meta := pluginMeta.Load()
	if meta == nil {
		fmt.Fprintf(os.Stderr, "no input, output, or custom plugin registered\n")
		return input.FLB_RETRY
	}

	if currInstance == nil {
		currInstance = newPluginInstance(*meta)
		if setupInstanceForTesting != nil {
			setupInstanceForTesting(currInstance)
		}
	}

	if err := currInstance.init(ptr); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		return flbReturnCode(err)
	}

	return input.FLB_OK
}

// FLBPluginInputPreRun is invoked by the fluent-bit runtime after the plugin has been
// initialized using FLBPluginRegister but before executing plugin callback functions.
//
//export FLBPluginInputPreRun
func FLBPluginInputPreRun(useHotReload C.int) int {
	registerWG.Wait()

	currInstanceMu.Lock()
	instance := currInstance
	currInstanceMu.Unlock()

	if instance == nil {
		fmt.Fprintf(os.Stderr, "plugin not initialized\n")
		return input.FLB_ERROR
	}

	if err := instance.resume(); err != nil {
		fmt.Fprintf(os.Stderr, "plugin pre-run error: %v\n", err)
		return flbReturnCode(err)
	}

	return input.FLB_OK
}

// FLBPluginInputPause this method gets invoked by the fluent-bit runtime, once the plugin has been
// paused, the plugin invoked this method and entering paused state.
//
//export FLBPluginInputPause
func FLBPluginInputPause() {
	currInstanceMu.Lock()
	instance := currInstance
	currInstanceMu.Unlock()

	if instance == nil {
		panic("plugin not initialized")
	}

	if instance.meta.input == nil {
		panic("can only pause input plugins")
	}

	if err := instance.stop(); err != nil {
		panic(err)
	}
}

// FLBPluginInputResume this method gets invoked by the fluent-bit runtime, once the plugin has been
// resumed, the plugin invoked this method and re-running state.
//
//export FLBPluginInputResume
func FLBPluginInputResume() {
	currInstanceMu.Lock()
	instance := currInstance
	currInstanceMu.Unlock()

	if instance == nil {
		panic("plugin not initialized")
	}

	if err := instance.resume(); err != nil {
		panic(err)
	}
}

// FLBPluginOutputPreExit this method gets invoked by the fluent-bit runtime, once the plugin has been
// exited, the plugin invoked this method and entering exiting state.
//
//export FLBPluginOutputPreExit
func FLBPluginOutputPreExit() {
	currInstanceMu.Lock()
	instance := currInstance
	currInstanceMu.Unlock()

	if instance == nil {
		panic("plugin not initialized")
	}

	if err := instance.outputPreExit(); err != nil {
		panic(err)
	}
}

// FLBPluginOutputPreRun -
//
//export FLBPluginOutputPreRun
func FLBPluginOutputPreRun(useHotReload C.int) int {
	registerWG.Wait()

	currInstanceMu.Lock()
	instance := currInstance
	currInstanceMu.Unlock()

	if instance == nil {
		panic("plugin not initialized")
	}

	if err := instance.resume(); err != nil {
		fmt.Fprintf(os.Stderr, "plugin pre-run error: %v\n", err)
		return flbReturnCode(err)
	}

	return input.FLB_OK
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
	currInstanceMu.Lock()
	instance := currInstance
	currInstanceMu.Unlock()

	if instance == nil {
		return input.FLB_RETRY
	}

	err := instance.inputCallback(data, csize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "callback error: %v\n", err)
	}

	return flbReturnCode(err)
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
	currInstanceMu.Lock()
	instance := currInstance
	currInstanceMu.Unlock()

	in := C.GoBytes(data, clength)
	tag := C.GoString(ctag)
	err := instance.outputFlush(tag, in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "plugin flush error: %v\n", err)
	}
	return flbReturnCode(err)
}

func marshalMsg(m Message) ([]byte, error) {
	return msgpack.Marshal([]any{&EventTime{m.Time}, m.Record})
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
		if err := msgpack.Unmarshal(entry[0], &eventWithMetadata); err != nil {
			return out, fmt.Errorf("msgpack unmarshal event with metadata: %w", err)
		}

		if len(eventWithMetadata) < 1 {
			return out, fmt.Errorf("msgpack unmarshal event time with metadata: expected 1 element, got %d", len(eventWithMetadata))
		}

		if err := msgpack.Unmarshal(eventWithMetadata[0], &eventTime); err != nil {
			return out, fmt.Errorf("msgpack unmarshal event time with metadata: %w", err)
		}

		return out, fmt.Errorf("msgpack unmarshal event time: %w", err)
	}

	var record map[string]any
	if err := msgpack.Unmarshal(entry[1], &record); err != nil {
		return out, fmt.Errorf("msgpack unmarshal event record: %w", err)
	}

	out.Time = eventTime.Time.UTC()
	out.Record = record
	out.tag = &tag

	return out, nil
}

// FLBPluginExit method is invoked once the plugin instance is exited from the fluent-bit context.
//
//export FLBPluginExit
func FLBPluginExit() int {
	currInstanceMu.Lock()
	defer currInstanceMu.Unlock()

	if currInstance != nil {
		currInstance.stop() //nolint:errcheck
		currInstance = nil
	}

	if unregister := unregisterFunc.Load(); unregister != nil {
		(*unregister)()
		unregisterFunc.Store(nil)
	}

	return input.FLB_OK
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

type flbCustomConfigLoader struct {
	ptr unsafe.Pointer
}

func (f *flbCustomConfigLoader) String(key string) string {
	return unquote(custom.FLBPluginConfigKey(f.ptr, key))
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

func testFLBPluginFlush(data []byte, tag string) int {
	cdata := C.CBytes(data)
	defer C.free(unsafe.Pointer(cdata))

	ctag := C.CString(tag)
	defer C.free(unsafe.Pointer(ctag))

	return FLBPluginFlush(cdata, C.int(len(data)), ctag)
}

// testInputCallback invokes inputCallback and returns the bytes outputted from it.
// This cannot be in the test file since test files can't use CGO.
func testInputCallback(inst *pluginInstance) ([]byte, error) {
	data := unsafe.Pointer(nil)
	var csize C.size_t

	err := inst.inputCallback(&data, &csize)

	if data == nil {
		return []byte{}, err
	}

	defer C.free(data)
	return C.GoBytes(data, C.int(csize)), err
}

func testFLBPluginInputCallback() ([]byte, int) {
	data := unsafe.Pointer(nil)
	var csize C.size_t

	retVal := FLBPluginInputCallback(&data, &csize)

	if data == nil {
		return []byte{}, retVal
	}

	defer C.free(data)
	return C.GoBytes(data, C.int(csize)), retVal
}
