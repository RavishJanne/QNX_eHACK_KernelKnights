/* fault_detector.c - rule based fault detection */
#include "common.h"

fault_t fault_detect(const node_info_t *n, uint64_t now)
{
    if (now - n->last_hb_ms > HB_TIMEOUT_MS) return FAULT_HEARTBEAT;
    if (n->task == TASK_FAILED)              return FAULT_TASK;
    if (n->cpu > CPU_LIMIT)                  return FAULT_CPU;
    if (n->ipc_us > IPC_LIMIT_US)            return FAULT_IPC;
    return FAULT_NONE;
}

const char *fault_name(fault_t f)
{
    static const char *s[] = { "NONE", "HEARTBEAT_LOST", "CPU_OVERLOAD", "IPC_LATENCY", "TASK_FAILED" };
    return s[f];
}

const char *task_name(task_state_t t)
{
    static const char *s[] = { "RUNNING", "BLOCKED", "FAILED" };
    return s[t];
}
