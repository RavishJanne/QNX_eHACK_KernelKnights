#ifndef COMMON_H
#define COMMON_H
#include <stdint.h>
#include <pthread.h>
#define NUM_NODES      3
#define PC_IP          "192.168.66.1"   /* <-- CHANGE: IP of PC running dashboard.py */
#define PC_PORT        5005
#define HB_TIMEOUT_MS  3000
#define CPU_LIMIT      90.0f
#define IPC_LIMIT_US   2000.0f

typedef enum { TASK_RUNNING = 0, TASK_BLOCKED, TASK_FAILED } task_state_t;
typedef enum { FAULT_NONE = 0, FAULT_HEARTBEAT, FAULT_CPU, FAULT_IPC, FAULT_TASK } fault_t;

typedef struct {
    int          id;
    float        cpu;         /* % */
    float        ipc_us;      /* IPC round-trip latency in microseconds */
    task_state_t task;
    uint32_t     heartbeat;
    uint64_t     last_hb_ms;
    fault_t      fault;
    float        cpu_avg;
    float        ipc_avg;
    float        health;      /* 0..100 */
} node_info_t;

extern node_info_t     g_nodes[NUM_NODES];
extern pthread_mutex_t g_lock;

uint64_t    now_ms(void);

/* node_sim.c */
void        node_sim_start(void);

/* fault_detector.c */
fault_t     fault_detect(const node_info_t *n, uint64_t now);
const char *fault_name(fault_t f);
const char *task_name(task_state_t t);

/* analytics.c */
void        analytics_update(node_info_t *n);

#endif
