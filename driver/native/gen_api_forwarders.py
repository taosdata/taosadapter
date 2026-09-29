#!/usr/bin/env python3

# Copyright (c) 2021 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Generate taosAdapter's public API forwarding layer (driver/native/tdApiForwarders.c).

Why: taosAdapter loads the native client driver itself (dlopen + tdNativeApiTable)
instead of linking libtaos.so, so something has to provide the `taos_*` / `tmq_*`
symbols the cgo code calls.  Those entry points are declared in the public client
header, and each forwarder resolves its counterpart from the loaded driver on
first use (see driver/native/tdNativeApi.h).

The list is taken from <taos.h> rather than from an adapter-side list, so it
cannot drift from the client ABI, and the parameter lists are copied verbatim --
the compiler then checks every definition against the declaration.

Usage:
    python3 driver/native/gen_api_forwarders.py [--header <taos.h>] [--out <out.c>] [--check]

Defaults: the in-tree public header (../../../../source/taos-community/include/client/taos.h)
and driver/native/tdApiForwarders.c next to this script.  --check regenerates in
memory and compares, so a stale file fails instead of being silently rewritten.
"""
import argparse
import os
import re
import sys

RELSRC = "driver/native/tdApiForwarders.c"
DEFAULT_HEADER = "../../../../source/taos-community/include/client/taos.h"

# How a forwarder reports that the driver does not provide its entry point.  The
# values mirror the dispatcher's CHECK_* macros in
# source/taos-community/source/client/wrapper/src/wrapperFunc.c, which set terrno and return -1 (int,
# int32_t, int64_t and the enum returns), false (bool) or NULL (pointers).
VOID_RET = "void"
CONFRET_RET = "setConfRet"
BOOL_RET = "bool"
INT_RETS = {"int", "int32_t", "int64_t", "tmq_res_t", "tmq_conf_res_t", "TSDB_SERVER_STATUS"}
# Return types that are pointers but do not end in '*'.
POINTER_TYPEDEFS = {"TAOS_ROW"}

# A parameter named like the slot / stub the generator emits (taos_stmt2_bind_param_a
# has one called `fp`, and a slot is td_api_slot_<name> / <name>_not_provided) would
# shadow it and silently break the forwarder, so the generator refuses to emit one.
INTERNAL_POINTER = "td_api_slot_"


def strip_comments(text):
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    return re.sub(r"//[^\n]*", "", text)


def read_declarations(header):
    """Every DLL_EXPORT declaration, as (return type, name, parameter list)."""
    with open(header, encoding="utf-8", errors="replace") as f:
        stripped = strip_comments(f.read())
    # Preprocessor lines first: the header defines DLL_EXPORT itself, and those
    # lines would otherwise be joined into the first declaration.
    lines = [(n, l) for n, l in enumerate(stripped.split("\n"), 1) if not l.lstrip().startswith("#")]

    decls = []
    i = 0
    while i < len(lines):
        if "DLL_EXPORT" not in lines[i][1]:
            i += 1
            continue

        start = lines[i][0]
        body = lines[i][1].split("DLL_EXPORT", 1)[1].strip()
        while ";" not in body:
            i += 1
            if i >= len(lines):
                raise SystemExit(f"[gen_api_forwarders] unterminated declaration at {header}:{start}")
            body = (body + " " + lines[i][1].strip()).strip()
        i += 1

        body = re.sub(r"\s+", " ", body).strip()
        m = re.match(r"^(.*?)\b([A-Za-z_]\w*)\s*\((.*)\)\s*;$", body)
        if m is None or not m.group(1).strip():
            raise SystemExit(f"[gen_api_forwarders] cannot parse declaration at {header}:{start}: {body}")
        decls.append((normalize_type(m.group(1)), m.group(2), m.group(3).strip()))

    if not decls:
        raise SystemExit(f"[gen_api_forwarders] no DLL_EXPORT declarations in {header}")
    return decls


def normalize_type(type_text):
    return re.sub(r"\s+", " ", type_text).strip()


def split_params(params):
    """Top level split of a parameter list, keeping declarators intact."""
    out, depth, current = [], 0, ""
    for ch in params:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            out.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():
        out.append(current.strip())
    return out


def param_declarator(param, index):
    """(declaration, argument) for one parameter.

    Parameters that carry no name in the header (a few typedef pointers such as
    `tmq_list_t *`) get one here: the definition is ours to write, and the
    compiler still checks it against the declaration."""
    if param == "...":
        return "...", None
    m = re.match(r"^(.*?)\b([A-Za-z_]\w*)\s*((?:\[\s*\])*)$", param)
    if m is None or not m.group(1).strip():
        name = f"arg{index}"
        return f"{param} {name}" if not param.rstrip().endswith("*") else f"{param}{name}", name

    if m.group(2).startswith(INTERNAL_POINTER) or m.group(2).endswith("_not_provided"):
        raise SystemExit(f"[gen_api_forwarders] parameter name {m.group(2)!r} would shadow the "
                         f"generated slot in: {param}")
    return param, m.group(2)


def return_kind(ret):
    if ret == VOID_RET:
        return "void"
    if ret == BOOL_RET:
        return "bool"
    if ret == CONFRET_RET:
        return "confret"
    if ret.endswith("*") or ret in POINTER_TYPEDEFS:
        return "ptr"
    if ret in INT_RETS:
        return "int"
    raise SystemExit(f"[gen_api_forwarders] unclassified return type {ret!r}: add it to the mapping")


def render(decls):
    out = [
        "/*",
        " * Copyright (c) 2025 TAOS Data, Inc.",
        " *",
        " * SPDX-License-Identifier: MIT",
        " *",
        " * Permission is hereby granted, free of charge, to any person obtaining a copy",
        " * of this software and associated documentation files (the \"Software\"), to deal",
        " * in the Software without restriction, including without limitation the rights",
        " * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell",
        " * copies of the Software, and to permit persons to whom the Software is",
        " * furnished to do so, subject to the following conditions:",
        " *",
        " * The above copyright notice and this permission notice shall be included in",
        " * all copies or substantial portions of the Software.",
        " *",
        " * THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR",
        " * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,",
        " * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE",
        " * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER",
        " * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,",
        " * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE",
        " * SOFTWARE.",
        " */",
        "",
        "/*",
        " * GENERATED by driver/native/gen_api_forwarders.py -- do not edit.",
        " *",
        " * taosAdapter loads the native client driver at run time (driver/native/tdApiLoader.c)",
        " * instead of linking libtaos.so, so the public C API the cgo code calls is provided",
        " * here: per entry point a slot that is filled from the driver once at start-up",
        " * (tdApiInstallSlots()) and a body that is nothing but `return slot(args);`.  A slot",
        " * the driver has no counterpart for keeps its <name>_not_provided stub, so the call",
        " * path carries no check and a missing entry point still fails only its own call.",
        " *",
        " * Variadic declarations (taos_options, taos_options_connection) are forwarded with",
        " * their fixed parameters only: the engine's implementations do not read the variadic",
        " * arguments (there is no va_arg anywhere in the client), and neither does any caller",
        " * here.  A declaration that ever does need them has to be forwarded through va_list",
        " * instead -- the generator does not emit that today.",
        " */",
        '#include "tdNativeApi.h"',
        "",
        "#include <taos.h>",
        "#include <stdbool.h>",
        "#include <stddef.h>",
        "#include <stdio.h>",
        "",
    ]

    entries = []
    for ret, name, params in decls:
        params_list = [p for p in split_params(params) if p != ""]
        if params_list == ["void"]:
            decl_params, args = "void", ""
        else:
            kinds = [param_declarator(p, i) for i, p in enumerate(params_list)]
            decl_params = ", ".join(d for d, _ in kinds) or "void"
            args = ", ".join(a for _, a in kinds if a is not None)

        kind = return_kind(ret)
        slot = f"td_api_slot_{name}"
        out.append(f"/* {ret} {name}({decl_params}) */")
        if kind == "void":
            out.append(f"static void {name}_not_provided({decl_params}) {{ tdApiNotProvided(\"{name}\"); }}")
            out.append(f"static __typeof__(&{name}) {slot} = {name}_not_provided;")
            out.append(f"void {name}({decl_params}) {{ {slot}({args}); }}")
        elif kind == "confret":
            out.append(f"static setConfRet {name}_not_provided({decl_params}) {{")
            out.append("  setConfRet ret = {.retCode = -1};")
            out.append(f"  tdApiNotProvided(\"{name}\");")
            out.append("  return ret;")
            out.append("}")
            out.append(f"static __typeof__(&{name}) {slot} = {name}_not_provided;")
            out.append(f"setConfRet {name}({decl_params}) {{ return {slot}({args}); }}")
        else:
            missing = {"int": "-1", "bool": "false", "ptr": "NULL"}[kind]
            out.append(f"static {ret} {name}_not_provided({decl_params}) {{")
            out.append(f"  tdApiNotProvided(\"{name}\");")
            out.append(f"  return {missing};")
            out.append("}")
            out.append(f"static __typeof__(&{name}) {slot} = {name}_not_provided;")
            out.append(f"{ret} {name}({decl_params}) {{ return {slot}({args}); }}")
        out.append("")
        entries.append((name, slot))

    out.append("/* The slots, for the one start-up pass that fills them. */")
    out.append("static const TdApiSlot tdApiSlots[] = {")
    for name, slot in entries:
        out.append(f'    {{ "{name}", (void **)&{slot} }},')
    out.append("    { NULL, NULL },")
    out.append("};")
    out.append("")
    out.append("/* Called once, by the loader, right after the driver is loaded: every slot")
    out.append(" * gets the driver's entry point, or keeps its not-provided stub.  This is the")
    out.append(" * only place that pays a lookup per entry point. */")
    out.append("void tdApiInstallSlots(void) {")
    out.append("  const char *missing[8] = {0};")
    out.append("  int32_t     missingCount = 0;")
    out.append("  int32_t     total = 0;")
    out.append("")
    out.append("  for (const TdApiSlot *entry = tdApiSlots; entry->name != NULL; ++entry) {")
    out.append("    ++total;")
    out.append("    void *fp = tdApiResolveLoaded(entry->name);")
    out.append("    if (fp == NULL) {")
    out.append("      if (missingCount < (int32_t)(sizeof(missing) / sizeof(missing[0]))) {")
    out.append("        missing[missingCount] = entry->name;")
    out.append("      }")
    out.append("      ++missingCount;")
    out.append("      continue;")
    out.append("    }")
    out.append("    *entry->slot = fp;")
    out.append("  }")
    out.append("")
    out.append("  if (missingCount != 0) {")
    out.append("    /* Said once, here, instead of on the call path: the affected calls return")
    out.append("     * their failure value and report the name through tdApiNotProvided(). */")
    out.append("    fprintf(stderr, \"taosadapter: %s does not provide %d of %d entry points\",")
    out.append("            tdApiDriverPath()[0] == '\\0' ? \"the loaded driver\" : tdApiDriverPath(), missingCount,")
    out.append("            total);")
    out.append("    for (int32_t i = 0; i < missingCount && i < (int32_t)(sizeof(missing) / sizeof(missing[0])); ++i) {")
    out.append("      fprintf(stderr, \"%s%s\", i == 0 ? \": \" : \", \", missing[i]);")
    out.append("    }")
    out.append("    fprintf(stderr, \"%s\\n\", missingCount > (int32_t)(sizeof(missing) / sizeof(missing[0])) ? \", ...\" : \"\");")
    out.append("  }")
    out.append("}")
    out.append("")
    return "\n".join(out)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(description="Generate (or verify) the public API forwarders.")
    parser.add_argument("--header", default=os.path.join(here, DEFAULT_HEADER),
                        help=f"public client header (default: {DEFAULT_HEADER})")
    parser.add_argument("--out", default=os.path.join(here, os.path.basename(RELSRC)),
                        help=f"output C file (default: {RELSRC})")
    parser.add_argument("--check", action="store_true",
                        help="do not write; fail when the output file is missing or differs")
    args = parser.parse_args()

    header = os.path.normpath(args.header)
    if not os.path.exists(header):
        raise SystemExit(f"[gen_api_forwarders] header not found: {header}")

    decls = read_declarations(header)
    content = render(decls)

    if args.check:
        if not os.path.exists(args.out):
            print(f"[gen_api_forwarders] MISSING -> {args.out}", file=sys.stderr)
            return 2
        with open(args.out, encoding="utf-8") as f:
            if f.read() != content:
                print(f"[gen_api_forwarders] OUT OF DATE -> {args.out} ({len(decls)} forwarders)")
                return 2
        print(f"[gen_api_forwarders] up to date ({len(decls)} forwarders) -> {args.out}")
        return 0

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"[gen_api_forwarders] wrote {len(decls)} forwarders -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
