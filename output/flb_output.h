/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit Go!
 *  ==============
 *  Copyright (C) 2015-2017 Treasure Data Inc.
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

#ifndef FLBGO_OUTPUT_H
#define FLBGO_OUTPUT_H

struct flb_api {
    char *(*output_get_property) (char *, void *);
    char *_;

    void *(*output_get_cmt_instance) (void *);
    void *__;

    void (*log_print) (int, const char*, int, const char*, ...);
    int *___;
    int (*output_log_check) (void *, int);

    char *____;
    int *_____;
};

struct flb_plugin_proxy_context {
    void *remote_context;
};

/* This structure is used for initialization.
 * It matches the one in proxy/go/go.c in fluent-bit source code.
 */
struct flbgo_output_plugin {
    void *_;
    struct flb_api *api;
    struct flb_output_instance *o_ins;
    struct flb_plugin_proxy_context *context;
};

char *output_get_property(char *key, void *plugin)
{
    struct flbgo_output_plugin *p = plugin;
    return p->api->output_get_property(key, p->o_ins);
}

void *output_get_cmt_instance(void *plugin)
{
    struct flbgo_output_plugin *p = plugin;
    return p->api->output_get_cmt_instance(p->o_ins);
}

void output_log_print_novar(void *plugin, int log_level, const char* message)
{
    struct flbgo_output_plugin *p = plugin;
    if (p->api->output_log_check(p->o_ins, log_level)) {
        /* all formating is done in golang, avoid fmt string bugs. */
        p->api->log_print(log_level, NULL, 0, "%s", message);
    }
}

#endif
