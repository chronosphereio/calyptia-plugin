/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2024 The Fluent Bit Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package custom

/*
#include <stdlib.h>
#include "flb_plugin.h"
#include "flb_custom.h"
*/
import "C"
import "unsafe"

// Define constants matching Fluent Bit core.
const (
	FLB_ERROR = C.FLB_ERROR
	FLB_OK    = C.FLB_OK
	FLB_RETRY = C.FLB_RETRY

	FLB_PROXY_CUSTOM_PLUGIN = C.FLB_PROXY_CUSTOM_PLUGIN
	FLB_PROXY_GOLANG        = C.FLB_PROXY_GOLANG
)

// Local type to define a plugin definition.
type (
	FLBPluginProxyDef C.struct_flb_plugin_proxy_def
	FLBCustomInstance C.struct_flb_custom_instance
)

// FLBPluginRegister when the FLBPluginInit is triggered by Fluent Bit, a plugin context
// is passed and the next step is to invoke this FLBPluginRegister() function
// to fill the required information: type, proxy type, flags name and
// description.
func FLBPluginRegister(def unsafe.Pointer, name, desc string) int {
	p := (*FLBPluginProxyDef)(def)
	p._type = FLB_PROXY_CUSTOM_PLUGIN
	p.proxy = FLB_PROXY_GOLANG
	p.flags = 0
	p.name = C.CString(name)
	p.description = C.CString(desc)
	return 0
}

func FLBPluginConfigKey(plugin unsafe.Pointer, key string) string {
	_key := C.CString(key)
	value := C.GoString(C.custom_get_property(_key, plugin))
	C.free(unsafe.Pointer(_key))
	return value
}

// Release resources allocated by the plugin initialization.
func FLBPluginUnregister(def unsafe.Pointer) {
	p := (*FLBPluginProxyDef)(def)
	C.free(unsafe.Pointer(p.name))
	C.free(unsafe.Pointer(p.description))
}

func FLBPluginLogPrint(plugin unsafe.Pointer, log_level C.int, message string) {
	_message := C.CString(message)
	C.custom_log_print_novar(plugin, log_level, _message)
	C.free(unsafe.Pointer(_message))
}
