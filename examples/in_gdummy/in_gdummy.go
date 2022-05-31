package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"time"
	"unsafe"

	"github.com/calyptia/plugin/input"
	"github.com/calyptia/cmetrics-go"
)

var cmt *cmetrics.Context
var counter_success *cmetrics.Counter
var counter_failure *cmetrics.Counter

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return input.FLBPluginRegister(def, "gdummy", "dummy GO!")
}

//export FLBPluginInit
// (fluentbit will call this)
// plugin (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(plugin unsafe.Pointer) int {
	var err error
	// Example to retrieve an optional configuration parameter
	param := input.FLBPluginConfigKey(plugin, "param")
	fmt.Printf("[flb-go] plugin parameter = '%s'\n", param)

	cmt, err = input.FLBPluginGetCMetricsContext(plugin)
	if err != nil {
		return input.FLB_ERROR
	}
	counter_success, err = cmt.CounterCreate("fluentbit", "input",
		"operation_succeeded_total", "Total number of succeeded operations", []string{"plugin_name"})
	if err != nil {
		return input.FLB_ERROR
	}

	counter_failure, err = cmt.CounterCreate("fluentbit", "input",
		"operation_failed_total", "Total number of failed operations", []string{"plugin_name"})
	if err != nil {
		return input.FLB_ERROR
	}

	return input.FLB_OK
}

//export FLBPluginInputCallback
func FLBPluginInputCallback(data *unsafe.Pointer, size *C.size_t) int {
	now := time.Now()
	flb_time := input.FLBTime{now}
	message := map[string]string{"message": "dummy"}

	entry := []interface{}{flb_time, message}

	enc := input.NewEncoder()
	packed, err := enc.Encode(entry)
	if err != nil {
		err = counter_failure.Inc(now, []string{"in_gdummy"})
		if err != nil {
			return input.FLB_ERROR
		}
		fmt.Println("Can't convert to msgpack:", message, err)
		return input.FLB_ERROR
	}

	length := len(packed)
	*data = C.CBytes(packed)
	*size = C.size_t(length)
	// For emitting interval adjustment.
	time.Sleep(1000 * time.Millisecond)

	err = counter_success.Inc(now, []string{"in_gdummy"})
	if err != nil {
		return input.FLB_ERROR
	}

	return input.FLB_OK
}

//export FLBPluginInputCleanupCallback
func FLBPluginInputCleanupCallback(data unsafe.Pointer) int {
	return input.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return input.FLB_OK
}

func main() {
}
