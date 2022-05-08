//
// Created by zhaoming.274 on 2022/4/15.
//

#include <mutex>
#include <thread>

#include "rocksdb/terark_namespace.h"
#include "util/sync_point.h"

namespace TERARKDB_NAMESPACE {

const char* kDebugMessageInstallSuccessfully = "InstallSuccessfully";
const char* kDebugMessageInstallFailed = "InstallFailed";
const char* kDebugMessageSkipCoreDump = "SkipCoreDump";

#if TERARKDB_DEBUG_LEVEL != 0

void DebugCallbackAbort(DebugCallbackType type, const char* msg) {
  fprintf(stderr, "Debug: type[%d] msg[%s]\n", type, msg);
  if (type != kDebugMessage) {
    std::abort();
  }
}

static void (*g_debug_callback)(DebugCallbackType type,
                                const char* msg) = &DebugCallbackAbort;
static std::once_flag g_debug_core_dump_flag;
static std::mutex g_debug_term_mutex;

void InvokeDebugCallback(DebugCallbackType type, const char* msg) {
  if (type == kDebugMessage) {
    g_debug_callback(kDebugMessage, msg);
    return;
  }
  bool dump_triggered = false;
  std::call_once(
      g_debug_core_dump_flag,
      [&dump_triggered](DebugCallbackType t, const char* m) {
        g_debug_callback(t, m);
        dump_triggered = true;
      },
      type, msg);
  if (!dump_triggered) {
    g_debug_callback(kDebugMessage, kDebugMessageSkipCoreDump);
  }
  if (type != kDebugCoreDump) {
    g_debug_term_mutex.lock();
    TEST_SYNC_POINT_CALLBACK("InvokeDebugCallback:Unlock", &g_debug_term_mutex);
  }
}
#else

void InvokeDebugCallback(DebugCallbackType type, const char* msg) {}

#endif

bool TerarkDBInstallDebugCallback(void (*debug_callback)(DebugCallbackType type,
                                                         const char* msg)) {
#if TERARKDB_DEBUG_LEVEL == 0
  if (debug_callback != nullptr) {
    debug_callback(kDebugMessage, kDebugMessageInstallFailed);
  }
  return false;
#else
  if (debug_callback == nullptr) {
    g_debug_callback = &DebugCallbackAbort;
  } else {
    g_debug_callback = debug_callback;
    g_debug_callback(kDebugMessage, kDebugMessageInstallSuccessfully);
  }
  return true;
#endif
}

}  // namespace TERARKDB_NAMESPACE