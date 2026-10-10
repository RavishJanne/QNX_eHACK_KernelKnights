/* analytics.c - moving averages and node health score */
#include "common.h"
#define ALPHA 0.2f
void analytics_update(node_info_t *n)
{
    n->cpu_avg=(n->cpu_avg==0)?n->cpu:ALPHA*n->cpu+(1-ALPHA)*n->cpu_avg;
    n->ipc_avg = (n->ipc_avg == 0) ? n->ipc_us : ALPHA * n->ipc_us + (1 - ALPHA) * n->ipc_avg;

    float h = 100.0f;
    if (n->cpu_avg > 50) h -= (n->cpu_avg - 50) * 0.8f;
    if (n->ipc_avg > 500) h -= (n->ipc_avg - 500) / 50.0f;
    if (n->task == TASK_BLOCKED) h -= 15;
    if (n->fault != FAULT_NONE)  h -= 40;

    if (h < 0)   h = 0;
    if (h > 100) h = 100;
    n->health = h;
}
