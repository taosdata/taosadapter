/*
 * Copyright (c) 2025 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef TD_NATIVE_API_H
#define TD_NATIVE_API_H

/*
 * taosAdapter's own loader for the native client driver.
 *
 * taosAdapter used to go through the libtaos.so dispatcher: it linked libtaos.so
 * for the public C API and libtaosnative.so for the entry points the dispatcher
 * did not forward.  That tied the adapter's build to whichever client package was
 * installed -- a client older than these sources fails to link, which is why the
 * version-skew fallback (-ltaosnative next to -ltaos) exists -- and it left the
 * answer to "which library owns taos_connect()" to ELF symbol lookup order.
 *
 * The adapter only ever wants the native driver, so it loads it the way the
 * dispatcher does: dlopen() the driver, read `tdNativeApiTable` from it, and
 * forward the public API through the function pointers it holds.  The driver has
 * exported that table since the dispatcher was made thin, and it keeps hiding the
 * public names, so loading it directly cannot shadow the dispatcher for anyone
 * else.
 *
 * What this buys:
 *   - only <taos.h> is needed at build time: no client library is linked, so an
 *     adapter built against a nightly client runs against any installed client
 *     (the table is read at run time, and a client too old to have the table is
 *     resolved per name with dlsym -- it exported the public API directly);
 *   - the driver is chosen by path, in the same order the dispatcher uses, and
 *     can be pinned with TDENGINE_DRIVER_PATH;
 *   - an entry point the installed driver does not provide fails that one call
 *     with a diagnostic instead of failing the build or crashing (see
 *     tdApiNotProvided() and native.Supported()).
 *
 * The forwarding layer is generated: see gen_api_forwarders.py, which reads the
 * public declarations out of <taos.h> so the generated definitions cannot drift
 * from the client ABI.
 */

#ifdef __cplusplus
extern "C" {
#endif

/* Load the native driver now and return 0, or -1 when it could not be loaded
 * (tdApiLoadError() then holds the reason).  Every other entry point loads it
 * on first use, so this is only needed to report a failure early. */
int tdApiLoad(void);

/* Human readable reason the driver could not be loaded; "" when it is loaded.
 * The pointer is a static buffer inside the loader: it stays valid for the life of
 * the process, but a later load attempt (or another thread's) rewrites its
 * contents, so copy it before relying on it. */
const char *tdApiLoadError(void);

/* Absolute path of the loaded driver; "" when it is not loaded.  Static buffer,
 * same lifetime rule as tdApiLoadError(). */
const char *tdApiDriverPath(void);

/* Resolve one public C API entry point of the driver: the API table first, then
 * dlsym() for a driver too old to export the table.  NULL when the driver is not
 * loaded or does not provide `name`.  Loads the driver on first use, so it is the
 * entry point the Go side calls (Supported() and friends).
 *
 * Thread safe, including on the first call from several threads at once: the load
 * runs under pthread_once / InitOnceExecuteOnce, and only the first caller performs
 * it.  Callers do not need to serialise the first call themselves. */
void *tdApiResolve(const char *name);

/* The same lookup without the load-on-first-use step: it only looks at the driver
 * that is loaded *now*.  It exists for code that runs inside the loader itself --
 * tdApiInstallSlots() is called from the loader's once-init, and pthread_once must
 * not be re-entered from its own init routine, so calling tdApiResolve() there
 * would deadlock.  Not thread safe on its own for the same reason: only call it
 * from the loader, or after tdApiLoad() has returned. */
void *tdApiResolveLoaded(const char *name);

/* Record `name` as not provided by the loaded driver.  Called by the generated
 * forwarders, which then return the failure value their return type uses.
 *
 * Best effort and deliberately unsynchronized: it runs on the call path of any
 * thread whose entry point is missing, and serializing it there was measured at
 * +4.6 ns per call uncontended plus a hard serialisation point under contention,
 * on a path a healthy driver never reaches.  The counters can therefore lose an
 * increment under concurrent calls and the recorded name can be observed while it
 * is being rewritten -- both only affect this diagnostic, never the value a call
 * returns. */
void tdApiNotProvided(const char *name);

/* Name of the last entry point that was recorded as not provided, or "" when every
 * call so far was resolved.  Mirrors the dispatcher's TSDB_CODE_DLL_FUNC_NOT_LOAD,
 * which only sets an error code and leaves the call site without a name.
 *
 * Recording stops after the first few calls (TD_NOT_PROVIDED_REPORT_MAX in
 * tdApiLoader.c), so this is the last name that was *reported*, not necessarily the
 * last one that failed.  Static buffer and best effort: see tdApiNotProvided(). */
const char *tdApiLastNotProvided(void);

/* How many calls were made to entry points the driver does not provide (not how
 * many entry points it lacks -- one missing entry point called a thousand times
 * counts a thousand times).  Best effort: see tdApiNotProvided(). */
int tdApiNotProvidedCount(void);

/*
 * The forwarders (generated into tdApiForwarders.c).
 *
 * The generated file defines, per public entry point:
 *   1. a "not provided" stub with the entry point's own signature, returning the
 *      failure value its return type uses -- mirroring the dispatcher's CHECK_*
 *      macros: -1 for the integer and enum returns (which set terrno to
 *      TSDB_CODE_DLL_*_NOT_LOAD there and record the name here), NULL for
 *      pointers, false for bool, a -1 retCode for setConfRet;
 *   2. a slot (a function pointer of the entry point's type) that starts at that
 *      stub;
 *   3. the entry point itself, which is nothing but
 *          return td_api_slot_x(args);
 *      i.e. one load and one indirect call, no check on the call path.
 *
 * tdApiInstallSlots() (also generated) then fills every slot from the loaded
 * driver in one pass, once, at start-up.  A driver that does not provide an entry
 * point keeps the stub in that slot, so the check costs nothing per call and a
 * missing entry point still fails just its own call.
 *
 * Why the work is done up front: measured with RDTSC, min-of-many, both ends in
 * the same shared library, the three shapes cost
 *
 *      call the exported symbol (PLT)      5.37 cycles/call
 *      slot filled at start-up, no check   5.38 cycles/call
 *      symbol that jmps to the slot        5.38 cycles/call   (what a normal
 *                                                              linked call costs)
 *      slot plus the check on every call   6.27 cycles/call   (what this file
 *                                                              used to generate)
 *
 * so filling the slots at start-up puts the per-call cost exactly where a normally
 * linked entry point is, and the check is what used to cost the extra ~0.9 cycles.
 */
typedef struct TdApiSlot {
  const char *name;
  /* Address of the generated slot.  The slot holds a void * at run time; it is
   * declared with its real function-pointer type in the generated file, so the
   * cast through void ** is only how the installer reaches it. */
  void      **slot;
} TdApiSlot;

#ifdef __cplusplus
}
#endif

#endif  // TD_NATIVE_API_H
