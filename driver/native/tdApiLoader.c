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

/*
 * Runtime loader for the native client driver.  See driver/native/tdNativeApi.h
 * for why taosAdapter loads the driver itself instead of linking libtaos.so.
 */

#if !defined(_WIN32) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE /* dladdr() */
#endif

#include "tdNativeApi.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
#include <windows.h>
#else
#include <dlfcn.h>
#include <limits.h>
#include <pthread.h>
#endif

#if defined(_WIN32)
#define TD_DRIVER_NAME "taosnative.dll"
#define TD_DIRSEP      "\\"
#elif defined(__APPLE__)
#define TD_DRIVER_NAME "libtaosnative.dylib"
#define TD_DIRSEP      "/"
#else
#define TD_DRIVER_NAME "libtaosnative.so"
#define TD_DIRSEP      "/"
#endif

/* Absolute path of the driver can be pinned with this, for a tree that keeps the
 * driver somewhere the search below does not know about (a build directory, say).
 * The dispatcher has no equivalent: it always sits next to the driver. */
#define TD_DRIVER_PATH_ENV "TDENGINE_DRIVER_PATH"

/* The driver's API table.  The layout has to match
 * source/taos-community/source/client/inc/taosNativeApi.h (TdNativeApiEntry: name,
 * function pointer, NULL-terminated).  That header is
 * private to the engine and is not installed with the client packages, which is
 * why the type is repeated here -- the driver is found by name at run time, so
 * nothing is checked by the linker. */
typedef struct TdNativeApiEntry {
  const char *name;
  void       *fn;
} TdNativeApiEntry;

#define TD_API_TABLE_SYM "tdNativeApiTable"

#define TD_PATH_MAX 4096
/* The candidate list is bounded, and the error text has room for it plus the
 * message around it, so the formats below cannot truncate. */
#define TD_ERR_MAX      1024
#define TD_LOAD_ERR_MAX (TD_ERR_MAX + 1024)
#define TD_NAME_MAX     128

/* How many "not provided" reports are printed before staying quiet: a hot path
 * calling a missing entry point must not flood stderr. */
#define TD_NOT_PROVIDED_REPORT_MAX 8

/* The counter is a diagnostic: saturate it instead of letting a long-running
 * process that keeps hitting a missing entry point overflow it (signed overflow
 * is undefined behaviour). */
#define TD_NOT_PROVIDED_COUNT_MAX 1000000

#if defined(_WIN32)
typedef HMODULE TdDriverHandle;

static TdDriverHandle tdDriverOpen(const char *path) { return LoadLibraryA(path); }
static void          *tdDriverSym(TdDriverHandle handle, const char *name) {
  return (void *)GetProcAddress(handle, name);
}
#else
typedef void *TdDriverHandle;

/* RTLD_LAZY: the engine loads its own optional helpers lazily, so a driver whose
 * dependencies are incomplete still starts (this mirrors taosLoadDll()).
 * RTLD_GLOBAL: with the driver in DT_NEEDED (which is how the adapter got it from
 * libtaos.so until now) its symbols land in the global scope, and client-side
 * extensions -- the enterprise encryption plugin, for one -- look the client up
 * with dlsym(RTLD_DEFAULT, ...).  dlopen() without RTLD_GLOBAL would take that
 * away. */
static TdDriverHandle tdDriverOpen(const char *path) { return dlopen(path, RTLD_LAZY | RTLD_GLOBAL); }
static void          *tdDriverSym(TdDriverHandle handle, const char *name) { return dlsym(handle, name); }
#endif

static TdDriverHandle         tsDriver = NULL;
static const TdNativeApiEntry *tsApiTable = NULL;
static char                   tsDriverPath[TD_PATH_MAX] = {0};
static char                   tsLoadError[TD_LOAD_ERR_MAX] = {0};
static char                   tsTried[TD_ERR_MAX] = {0};
static char                   tsLastNotProvided[TD_NAME_MAX] = {0};
static int                    tsNotProvidedCount = 0;
static int                    tsNotProvidedReported = 0;

#if defined(_WIN32)
static INIT_ONCE tsLoadOnce = INIT_ONCE_STATIC_INIT;
#else
static pthread_once_t tsLoadOnce = PTHREAD_ONCE_INIT;
#endif

/* Defined in the generated tdApiForwarders.c: fills one slot per public entry
 * point from the driver that was just loaded. */
extern void tdApiInstallSlots(void);

/* Records what the search looked at, so a failure names the paths instead of
 * just saying the library is missing.  Bounded: a long LD_LIBRARY_PATH can fill the
 * text, and a path that does not fit is dropped rather than truncated (the buffer is
 * never overrun -- the separator and the path are measured after it is added). */
static void tdRecordTried(const char *path) {
  size_t used = strlen(tsTried);
  if (used != 0 && used + 2 < sizeof(tsTried)) {
    tsTried[used++] = ',';
    tsTried[used++] = ' ';
    tsTried[used] = '\0';
  }
  if (used + strlen(path) < sizeof(tsTried)) {
    strcat(tsTried, path);
  }
}

static void tdSetLoadError(const char *format, const char *detail) {
  snprintf(tsLoadError, sizeof(tsLoadError), format, detail, tsTried);
}

/* Directory holding the running program, i.e. the adapter binary.  The dispatcher
 * uses the directory of the library holding it instead (libtaos.so always sits
 * next to the driver); for the adapter the executable plays that role. */
static int tdExeDir(char *dir, size_t dirLen) {
#if defined(_WIN32)
  char  path[TD_PATH_MAX] = {0};
  DWORD n = GetModuleFileNameA(NULL, path, (DWORD)sizeof(path));
  if (n == 0 || n >= sizeof(path)) {
    return -1;
  }
#else
  char path[TD_PATH_MAX] = {0};
  Dl_info info = {0};
  if (dladdr((void *)tdExeDir, &info) == 0 || info.dli_fname == NULL) {
    return -1;
  }
  snprintf(path, sizeof(path), "%s", info.dli_fname);
#endif

  char *sep = strrchr(path, TD_DIRSEP[0]);
  if (sep == NULL || sep == path) {
    return -1;
  }
  *sep = '\0';
  if (strlen(path) >= dirLen) {
    return -1;
  }
  snprintf(dir, dirLen, "%s", path);
  return 0;
}

static int tdJoinPath(char *out, size_t outLen, const char *dir, const char *name) {
  int n = snprintf(out, outLen, "%s%s%s", dir, TD_DIRSEP, name);
  return (n <= 0 || (size_t)n >= outLen) ? -1 : 0;
}

/* Open one candidate.  The path recorded for diagnostics is the file the loader
 * actually mapped, not the candidate string: dlopen() remembers the name it was
 * handed and sanitizers and crash reports print that verbatim, so a path built
 * from a search-path entry would show up as the caller spelled it (the test
 * harnesses keep "<test dir>/../../../" forms).  The dispatcher resolves the
 * same problem up front in taosResolveDriverPath(); here the driver is already
 * loaded, so the table symbol it exports tells us where it came from. */
static void tdRecordDriverPath(TdDriverHandle handle, const char *candidate) {
#if defined(_WIN32)
  (void)handle;
  snprintf(tsDriverPath, sizeof(tsDriverPath), "%s", candidate);
#else
  char    resolved[TD_PATH_MAX] = {0};
  void   *anchor = tdDriverSym(handle, TD_API_TABLE_SYM);
  Dl_info info = {0};

  if (anchor != NULL && dladdr(anchor, &info) != 0 && info.dli_fname != NULL &&
      realpath(info.dli_fname, resolved) != NULL) {
    snprintf(tsDriverPath, sizeof(tsDriverPath), "%s", resolved);
  } else if (realpath(candidate, resolved) != NULL) {
    snprintf(tsDriverPath, sizeof(tsDriverPath), "%s", resolved);
  } else {
    snprintf(tsDriverPath, sizeof(tsDriverPath), "%s", candidate);
  }
#endif
}

static int tdTryDriver(const char *candidate) {
  tdRecordTried(candidate);

  TdDriverHandle handle = tdDriverOpen(candidate);
  if (handle == NULL) {
    return -1;
  }

  tsDriver = handle;
  tdRecordDriverPath(handle, candidate);
  return 0;
}

static int tdTryDriverInDir(const char *dir, const char *name) {
  char path[TD_PATH_MAX] = {0};
  if (tdJoinPath(path, sizeof(path), dir, name) != 0) {
    return -1;
  }
  return tdTryDriver(path);
}

/* The LD_LIBRARY_PATH / DYLD_LIBRARY_PATH entries, canonicalized: the loader
 * would find the driver by itself, but then the recorded name comes from the
 * search-path entry, which the test harnesses keep in a "<test dir>/../../.."
 * form (see the same reasoning in
 * source/taos-community/source/client/wrapper/src/wrapperDriver.c). */
static int tdTryDriverInSearchPath(const char *name) {
#if defined(_WIN32)
  return -1;
#else
  const char *vars[] = {"LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH"};
  int         loaded = -1;

  for (size_t i = 0; i < sizeof(vars) / sizeof(vars[0]) && loaded != 0; ++i) {
    const char *list = getenv(vars[i]);
    if (list == NULL || list[0] == '\0') {
      continue;
    }

    char *copy = strdup(list);
    if (copy == NULL) {
      continue;
    }

    char *save = NULL;
    for (char *dir = strtok_r(copy, ":", &save); dir != NULL; dir = strtok_r(NULL, ":", &save)) {
      if (tdTryDriverInDir(dir[0] == '\0' ? "." : dir, name) == 0) {
        loaded = 0;
        break;
      }
    }

    free(copy);
  }

  return loaded;
#endif
}

static void tdLoad(void) {
  const char *name = TD_DRIVER_NAME;

  /* An explicit path wins, and a broken one is reported as such instead of
   * falling back: it is how a test or a deployment pins the driver. */
  const char *configured = getenv(TD_DRIVER_PATH_ENV);
  if (configured != NULL && configured[0] != '\0') {
    if (tdTryDriver(configured) != 0) {
      tdSetLoadError("cannot load %s (from " TD_DRIVER_PATH_ENV "); tried: %s", name);
      return;
    }
  }

  /* Search order, the same one taosDriverInit() uses in the dispatcher, except that
   * the running program (not the library holding the loader) is what "next to"
   * means here:
   *   1. next to the running program      (installed side by side)
   *   2. <exe dir>/../lib                 (build tree: build/bin next to build/lib)
   *   3. <exe dir>/../driver              (package layout: <prefix>/bin + <prefix>/driver)
   *   4. LD_LIBRARY_PATH / DYLD_LIBRARY_PATH
   *   5. the loader's own search (ldconfig cache, /usr/lib, PATH on Windows)
   * macOS adds /usr/local/lib after those. */
  char exeDir[TD_PATH_MAX] = {0};
  if (tsDriver == NULL && tdExeDir(exeDir, sizeof(exeDir)) == 0) {
    (void)tdTryDriverInDir(exeDir, name);
  }

  if (tsDriver == NULL && exeDir[0] != '\0') {
    /* One buffer for both derived candidates: appending a suffix to a path that
     * already fills TD_PATH_MAX must not truncate (and gcc warns if it could). */
    char up[TD_PATH_MAX + 32] = {0};
#if defined(_WIN32)
    (void)snprintf(up, sizeof(up), "%s" TD_DIRSEP ".." TD_DIRSEP "lib", exeDir);
#else
    (void)snprintf(up, sizeof(up), "%s/../lib", exeDir);
#endif
    (void)tdTryDriverInDir(up, name);
  }

  if (tsDriver == NULL && exeDir[0] != '\0') {
    char up[TD_PATH_MAX + 32] = {0};
    (void)snprintf(up, sizeof(up), "%s" TD_DIRSEP ".." TD_DIRSEP "driver", exeDir);
    (void)tdTryDriverInDir(up, name);
  }

  if (tsDriver == NULL) {
    (void)tdTryDriverInSearchPath(name);
  }

  if (tsDriver == NULL) {
    (void)tdTryDriver(name);
  }

#if defined(__APPLE__)
  if (tsDriver == NULL) {
    (void)tdTryDriverInDir("/usr/local/lib", name);
  }
#endif

  if (tsDriver == NULL) {
    tdSetLoadError("cannot load %s; tried: %s", name);
    return;
  }

  tsApiTable = (const TdNativeApiEntry *)tdDriverSym(tsDriver, TD_API_TABLE_SYM);
  if (tsApiTable != NULL && tsApiTable->name == NULL) {
    /* An empty table cannot answer anything; fall back to dlsym() per name. */
    tsApiTable = NULL;
  }

  /* Hand every generated slot its entry point now, once: after this the call path
   * is a load and an indirect call, with no lookup and no check (see
   * tdNativeApi.h).  Slots the driver has no counterpart for keep their
   * not-provided stub, and that is reported here rather than per call. */
  tdApiInstallSlots();

  tsLoadError[0] = '\0';
}

#if defined(_WIN32)
static BOOL CALLBACK tdLoadCallback(PINIT_ONCE once, PVOID param, PVOID *ctx) {
  (void)once;
  (void)param;
  (void)ctx;
  tdLoad();
  return TRUE;
}
#else
static void tdLoadCallback(void) { tdLoad(); }
#endif

static void tdLoadEnsure(void) {
#if defined(_WIN32)
  (void)InitOnceExecuteOnce(&tsLoadOnce, tdLoadCallback, NULL, NULL);
#else
  (void)pthread_once(&tsLoadOnce, tdLoadCallback);
#endif
}

int tdApiLoad(void) {
  tdLoadEnsure();
  return tsDriver == NULL ? -1 : 0;
}

const char *tdApiLoadError(void) { return tsLoadError; }

const char *tdApiDriverPath(void) { return tsDriverPath; }

void *tdApiResolveLoaded(const char *name) {
  if (name == NULL || tsDriver == NULL) {
    return NULL;
  }

  if (tsApiTable != NULL) {
    for (const TdNativeApiEntry *entry = tsApiTable; entry->name != NULL; ++entry) {
      if (strcmp(entry->name, name) == 0) {
        return entry->fn;
      }
    }
  }

  /* A client older than the API table, or an entry point the dispatcher never
   * forwarded: those still export the public names, so dlsym() finds them. */
  return tdDriverSym(tsDriver, name);
}

void *tdApiResolve(const char *name) {
  if (name == NULL) {
    return NULL;
  }

  tdLoadEnsure();
  return tdApiResolveLoaded(name);
}

void tdApiNotProvided(const char *name) {
  if (name == NULL) {
    return;
  }

  /* Deliberately unsynchronized (see tdNativeApi.h): these are diagnostics, and a
   * lost increment or a name read while it is rewritten changes only what
   * tdApiNotProvidedCount()/tdApiLastNotProvided() report -- never the value a call
   * returns.  The counter saturates instead of overflowing. */
  if (tsNotProvidedCount < TD_NOT_PROVIDED_COUNT_MAX) {
    ++tsNotProvidedCount;
  }

  /* Nothing else reports this: a missing entry point only shows up as the -1 / NULL
   * the forwarder returns, with no explanation.  Report the first few calls, then
   * stay quiet -- a hot path calling a missing entry point must not flood stderr.
   *
   * The name is recorded only while that budget lasts, which is what keeps the call
   * path cheap once the budget is gone: after TD_NOT_PROVIDED_REPORT_MAX calls a
   * caller that keeps hitting a missing entry point pays a single increment, not a
   * snprintf() per call.  The budget is global, not per name, and the name it leaves
   * behind is the last one that was reported. */
  if (tsNotProvidedReported < TD_NOT_PROVIDED_REPORT_MAX) {
    ++tsNotProvidedReported;
    snprintf(tsLastNotProvided, sizeof(tsLastNotProvided), "%s", name);
    fprintf(stderr,
            "taosadapter: %s does not provide %s (driver: %s), the call returns an error. "
            "Install a matching TDengine client.\n",
            TD_DRIVER_NAME, name, tsDriverPath[0] == '\0' ? "<not loaded>" : tsDriverPath);
  }
}

const char *tdApiLastNotProvided(void) { return tsLastNotProvided; }

int tdApiNotProvidedCount(void) { return tsNotProvidedCount; }
