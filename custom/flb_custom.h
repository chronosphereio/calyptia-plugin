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

#ifndef FLBGO_CUSTOM_H
#define FLBGO_CUSTOM_H

struct flb_api {
    char *_;
    char *__;

    void *___;
    void *____;

    void (*log_print) (int, const char*, int, const char*, ...);
    int *_____;
    int *______;

    char *(*custom_get_property) (char *, void *);
    int (*custom_log_check) (void *, int);
};

struct flb_plugin_proxy_context {
    void *remote_context;
};

/* This structure is used for initialization.
 * It matches the one in proxy/go/go.c in fluent-bit source code.
 */
struct flbgo_custom_plugin {
    void *_;
    struct flb_api *api;
    struct flb_custom_instance *c_ins;
    struct flb_plugin_proxy_context *context;
};

char *custom_get_property(char *key, void *plugin)
{
    struct flbgo_custom_plugin *p = plugin;
    return p->api->custom_get_property(key, p->c_ins);
}

void custom_log_print_novar(void *plugin, int log_level, const char* message)
{
    struct flbgo_custom_plugin *p = plugin;
    if (p->api->custom_log_check(p->c_ins, log_level)) {
        /* all formating is done in golang, avoid fmt string bugs. */
        p->api->log_print(log_level, NULL, 0, "%s", message);
    }
}

#endif
