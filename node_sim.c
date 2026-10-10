/* node_sim.c - simulates nodes; IPC latency is measured with real QNX MsgSend() */
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/neutrino.h>
#include "common.h"

node_info_t     g_nodes[NUM_NODES];
pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;

static int g_chid;

uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* Echo server: replies to every message (measures kernel IPC round trip) */
static void *echo_server(void *arg)
{
    char buf[64];
    int rcvid;
    (void)arg;
    for (;;) {
        rcvid = MsgReceive(g_chid, buf, sizeof(buf), NULL);
        if (rcvid > 0) MsgReply(rcvid, 0, buf, sizeof(buf));
    }
    return NULL;
}

static float measure_ipc_us(int coid)
{
    char msg[64] = "ping", reply[64];
    struct timespec a, b;
    clock_gettime(CLOCK_MONOTONIC, &a);
    if (MsgSend(coid, msg, sizeof(msg), reply, sizeof(reply)) == -1) return 99999.0f;
    clock_gettime(CLOCK_MONOTONIC, &b);
    return (b.tv_sec - a.tv_sec) * 1e6f + (b.tv_nsec - a.tv_nsec) / 1e3f;
}

static void *node_thread(void *arg)
{
    int id = (int)(intptr_t)arg;
    unsigned seed = (unsigned)time(NULL) + id * 77;
    float cpu = 20 + id * 10;
    int coid = ConnectAttach(0, 0, g_chid, _NTO_SIDE_CHANNEL, 0);

    for (;;) {
        uint64_t t = now_ms();
        unsigned sec = (unsigned)(t / 1000);

        /* random-walk CPU */
        cpu += ((rand_r(&seed) % 100) - 50) / 25.0f;
        if (cpu < 5)  cpu = 5;
        if (cpu > 60) cpu = 60;

        float shown_cpu = cpu;
        task_state_t task = TASK_RUNNING;
        int send_hb = 1;

        /* ---- fault injection so the dashboard has something to show ---- */
        if (id == 1 && (sec % 40) < 8)  send_hb = 0;          /* heartbeat loss */
        if (id == 2 && (sec % 55) < 6)  shown_cpu = 95.0f;    /* CPU overload   */
        if (id == 0 && (sec % 70) < 5)  task = TASK_FAILED;   /* task crash     */
        if (id == 0 && (sec % 70) >= 5 && (sec % 70) < 8) task = TASK_BLOCKED;
        if (id == 2 && (sec % 90) >= 40 && (sec % 90) < 44) usleep(3000); /* slow IPC */

        float ipc = measure_ipc_us(coid);

        pthread_mutex_lock(&g_lock);
        node_info_t *n = &g_nodes[id];
        n->cpu    = shown_cpu;
        n->ipc_us = ipc;
        n->task   = task;
        if (send_hb) { n->heartbeat++; n->last_hb_ms = t; }
        pthread_mutex_unlock(&g_lock);

        usleep(500 * 1000);
    }
    return NULL;
}

void node_sim_start(void)
{
    pthread_t th;
    g_chid = ChannelCreate(0);
    if (g_chid == -1) { perror("ChannelCreate"); exit(1); }
    pthread_create(&th, NULL, echo_server, NULL);

    for (int i = 0; i < NUM_NODES; i++) {
        g_nodes[i].id = i;
        g_nodes[i].last_hb_ms = now_ms();
        g_nodes[i].health = 100;
        pthread_create(&th, NULL, node_thread, (void *)(intptr_t)i);
    }
}
