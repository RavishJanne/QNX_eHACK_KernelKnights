/* monitor.c - main: collects node data, runs fault detection + analytics, sends JSON over UDP */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "common.h"

int main(void)
{
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    struct sockaddr_in dst;
    memset(&dst, 0, sizeof(dst));
    dst.sin_family = AF_INET;
    dst.sin_port   = htons(PC_PORT);
    inet_pton(AF_INET, PC_IP, &dst.sin_addr);

    node_sim_start();
    printf("QNX monitor started -> %s:%d\n", PC_IP, PC_PORT);

    for (;;) {
        sleep(1);
        uint64_t now = now_ms();

        for (int i = 0; i < NUM_NODES; i++) {
            char json[320];

            pthread_mutex_lock(&g_lock);
            node_info_t n = g_nodes[i];
            n.fault = fault_detect(&n, now);
            analytics_update(&n);
            g_nodes[i].fault   = n.fault;
            g_nodes[i].cpu_avg = n.cpu_avg;
            g_nodes[i].ipc_avg = n.ipc_avg;
            g_nodes[i].health  = n.health;
            pthread_mutex_unlock(&g_lock);

            snprintf(json, sizeof(json),
                "{\"node\":%d,\"ts\":%llu,\"cpu\":%.1f,\"cpu_avg\":%.1f,"
                "\"ipc_us\":%.1f,\"ipc_avg\":%.1f,\"task\":\"%s\","
                "\"hb\":%u,\"hb_age_ms\":%llu,\"fault\":\"%s\",\"health\":%.1f}",
                n.id, (unsigned long long)now, n.cpu, n.cpu_avg,
                n.ipc_us, n.ipc_avg, task_name(n.task),
                (unsigned)n.heartbeat, (unsigned long long)(now - n.last_hb_ms),
                fault_name(n.fault), n.health);

            sendto(sock, json, strlen(json), 0, (struct sockaddr *)&dst, sizeof(dst));
            printf("%s\n", json);
        }
    }
    return 0;
}
