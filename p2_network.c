/* ================================================================ */
/* QNX  —  Pi2  PROCESS 1 :  NETWORK RECEIVER                      */
/* Raspberry Pi 2 — Arm Side                                        */
/*                                                                  */
/* Threads:                                                         */
/*   TCP RX Thread (PRIO 60) — TCP server on port 5000,             */
/*                              receives ControlPackets from Pi1,   */
/*                              validates, timestamps, publishes     */
/*                              to /mq/control_targets              */
/*                                                                  */
/* Bidirectional socket:                                            */
/*   Pi1 → Pi2 : ControlPacket (fixed 36 bytes)                     */
/*   Pi2 → Pi1 : AlertPacket   (fixed  8 bytes) — sent when         */
/*               Process 2 notifies a servo limit via the same MQ   */
/*                                                                  */
/* IPC output:                                                      */
/*   POSIX Message Queue  /mq/control_targets  (to Process 2)       */
/*                                                                  */
/* IPC input (alert path):                                          */
/*   POSIX Message Queue  /mq/alert_packets    (from Process 2)     */
/*   Consumed here and forwarded as AlertPacket back to Pi1.        */
/* ================================================================ */

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>
#include <time.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <mqueue.h>
#include <sys/neutrino.h>

#ifndef be64toh
  #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    #define be64toh(x) __builtin_bswap64(x)
  #else
    #define be64toh(x) (x)
  #endif
#endif

/* ================================================================ */
/* CONFIG                                                           */
/* ================================================================ */
#define SERVER_PORT         5000

/* POSIX MQs */
#define MQ_CONTROL_TARGETS  "/mq/control_targets"   /* P1 → P2 (targets) */
#define MQ_ALERT_PACKETS    "/mq/alert_packets"     /* P2 → P1 (alerts)  */
#define MQ_MAXMSG           8
#define TCP_THREAD_PRIO     60
#define ALERT_FWD_PRIO      58   /* alert forward thread priority */

/* ================================================================ */
/* ALERT PACKET  (Pi2 → Pi1, must match pi1_process2_network_haptics.c) */
/* ================================================================ */
#define ALERT_MAGIC  0xDEADC0DEu

typedef struct __attribute__((packed))
{
    uint32_t magic;
    uint8_t  alert_id;
    uint8_t  _pad[3];
} AlertPacket;

/* ================================================================ */
/* CONTROL PACKET  (Pi1 → Pi2, wire format)                         */
/* ================================================================ */
typedef struct __attribute__((packed))
{
    uint32_t sequence;
    uint32_t padding;
    uint64_t timestamp_ns;
    float    servo1_deg;
    float    servo2_deg;
    float    servo4_deg;
    uint8_t  gripper;
    uint8_t  _pad[3];
} ControlPacket;

/* ================================================================ */
/* IPC MESSAGES                                                     */
/* ================================================================ */

/* Published to /mq/control_targets → Process 2 servo controller */
typedef struct __attribute__((packed))
{
    float    servo1_deg;
    float    servo2_deg;
    float    servo4_deg;
    uint8_t  gripper;
    uint8_t  _pad[3];
} TargetMqMsg;

/* Consumed from /mq/alert_packets ← Process 2 servo controller */
typedef struct __attribute__((packed))
{
    uint8_t  alert_id;   /* ALERT_Sx_{MAX|MIN} */
    uint8_t  _pad[3];
} AlertMqMsg;

/* ================================================================ */
/* SHARED STATE                                                     */
/* ================================================================ */
static _Atomic int link_active  = 0;

/* Client socket — set by tcp_rx_thread, read by alert_fwd_thread */
static int             client_sock = -1;
static pthread_mutex_t sock_mutex  = PTHREAD_MUTEX_INITIALIZER;

/* MQ handles */
static mqd_t mq_targets = (mqd_t)-1;   /* write → Process 2 */
static mqd_t mq_alerts  = (mqd_t)-1;   /* read  ← Process 2 */

/* ================================================================ */
/* HELPERS                                                          */
/* ================================================================ */
static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec * 1000000000ULL) + ts.tv_nsec;
}

static void set_fifo_priority(int prio)
{
    struct sched_param sp;
    memset(&sp, 0, sizeof(sp));
    sp.sched_priority = prio;
    sched_setscheduler(0, SCHED_FIFO, &sp);
}

static int recv_all(int fd, void *buf, size_t len)
{
    uint8_t *p = buf;
    while (len > 0) {
        ssize_t n = recv(fd, p, len, 0);
        if (n <= 0) return -1;
        p += n;  len -= n;
    }
    return 0;
}

static int send_all(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    while (len > 0) {
        ssize_t n = send(fd, p, len, 0);
        if (n <= 0) return -1;
        p += n;  len -= n;
    }
    return 0;
}

/* ================================================================ */
/* ALERT FORWARD THREAD  (PRIO 58)                                  */
/*                                                                  */
/* Blocks on /mq/alert_packets waiting for AlertMqMsg from Process  */
/* 2. Each message is repacked into an AlertPacket and forwarded    */
/* back to Pi1 on the live TCP socket.                              */
/*                                                                  */
/* Runs as a sibling to tcp_rx_thread. When the socket closes,     */
/* mq_receive() continues to run but send_all() will fail — the    */
/* thread then spins on mq_receive and drops alerts until a new     */
/* socket is available. This is acceptable: Pi1 will reconnect.    */
/* ================================================================ */
static void *alert_fwd_thread(void *arg)
{
    (void)arg;
    set_fifo_priority(ALERT_FWD_PRIO);

    printf("[P1-ALERT FWD] Ready — forwarding Pi2 servo alerts to Pi1\n");

    while (1)
    {
        AlertMqMsg amsg;
        ssize_t r = mq_receive(mq_alerts,
                               (char *)&amsg, sizeof(amsg), NULL);
        if (r < 0) {
            perror("[P1-ALERT FWD] mq_receive");
            usleep(10000);
            continue;
        }

        /* Grab live socket under mutex */
        int fd;
        pthread_mutex_lock(&sock_mutex);
        fd = client_sock;
        pthread_mutex_unlock(&sock_mutex);

        if (fd < 0)
            continue;   /* no client connected — discard */

        AlertPacket ap;
        ap.magic    = htonl(ALERT_MAGIC);
        ap.alert_id = amsg.alert_id;
        memset(ap._pad, 0, sizeof(ap._pad));

        if (send_all(fd, &ap, sizeof(ap)) < 0)
            printf("[P1-ALERT FWD] send failed — client likely disconnected\n");
    }
    return NULL;
}

/* ================================================================ */
/* TCP RX THREAD  (PRIO 60)                                         */
/*                                                                  */
/* Accepts one client at a time (Pi1).                              */
/* For each received ControlPacket:                                 */
/*   1. Decode from network byte order.                             */
/*   2. Compute and log latency.                                    */
/*   3. Publish TargetMqMsg to /mq/control_targets.                */
/* ================================================================ */
static void *tcp_rx_thread(void *arg)
{
    (void)arg;
    set_fifo_priority(TCP_THREAD_PRIO);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); return NULL; }

    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(SERVER_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind  (listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        { perror("bind");   return NULL; }
    if (listen(listen_fd, 1) < 0)
        { perror("listen"); return NULL; }

    printf("[P1] TCP SERVER LISTENING ON PORT %d\n", SERVER_PORT);

    while (1)
    {
        struct sockaddr_in client_addr;
        socklen_t clen = sizeof(client_addr);
        int new_fd = accept(listen_fd,
                            (struct sockaddr *)&client_addr, &clen);
        if (new_fd < 0) continue;

        printf("[P1] CLIENT CONNECTED: %s\n",
               inet_ntoa(client_addr.sin_addr));
        setsockopt(new_fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

        pthread_mutex_lock(&sock_mutex);
        client_sock = new_fd;
        pthread_mutex_unlock(&sock_mutex);

        atomic_store(&link_active, 1);

        while (1)
        {
            ControlPacket pkt;
            if (recv_all(new_fd, &pkt, sizeof(pkt)) < 0)
            {
                printf("[P1] CLIENT DISCONNECTED\n");
                atomic_store(&link_active, 0);

                pthread_mutex_lock(&sock_mutex);
                client_sock = -1;
                pthread_mutex_unlock(&sock_mutex);

                close(new_fd);
                break;
            }

            /* Latency measurement */
            uint64_t recv_ns  = now_ns();
            uint64_t sent_ns  = be64toh(pkt.timestamp_ns);
            uint64_t lat_us   = (recv_ns - sent_ns) / 1000ULL;

            printf("[P1] SEQ=%u LAT=%llu us | S1=%.1f S2=%.1f S4=%.1f GRIP=%d\n",
                   ntohl(pkt.sequence),
                   (unsigned long long)lat_us,
                   (double)pkt.servo1_deg,
                   (double)pkt.servo2_deg,
                   (double)pkt.servo4_deg,
                   (int)pkt.gripper);

            /* Publish decoded targets to Process 2 */
            TargetMqMsg tmsg;
            tmsg.servo1_deg = pkt.servo1_deg;
            tmsg.servo2_deg = pkt.servo2_deg;
            tmsg.servo4_deg = pkt.servo4_deg;
            tmsg.gripper    = pkt.gripper;
            memset(tmsg._pad, 0, sizeof(tmsg._pad));

            if (mq_send(mq_targets,
                        (const char *)&tmsg, sizeof(tmsg), 0) == -1)
            {
                if (errno != EAGAIN)
                    perror("[P1] mq_send control_targets");
            }
        }
    }
    return NULL;
}

/* ================================================================ */
/* MAIN                                                             */
/* ================================================================ */
int main(void)
{
    mlockall(MCL_CURRENT | MCL_FUTURE);

    /* ── Create /mq/control_targets (Process 2 reads this) ── */
    struct mq_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.mq_flags   = O_NONBLOCK;
    attr.mq_maxmsg  = MQ_MAXMSG;
    attr.mq_msgsize = sizeof(TargetMqMsg);

    mq_targets = mq_open(MQ_CONTROL_TARGETS,
                         O_CREAT | O_WRONLY | O_NONBLOCK,
                         0660, &attr);
    if (mq_targets == (mqd_t)-1)
        { perror("mq_open control_targets"); return 1; }

    /* ── Open /mq/alert_packets (Process 2 creates this) ── */
    struct mq_attr aattr;
    memset(&aattr, 0, sizeof(aattr));
    aattr.mq_flags   = 0;   /* blocking */
    aattr.mq_maxmsg  = MQ_MAXMSG;
    aattr.mq_msgsize = sizeof(AlertMqMsg);

    while ((mq_alerts = mq_open(MQ_ALERT_PACKETS, O_RDONLY)) == (mqd_t)-1)
    {
        printf("[P1] Waiting for %s (Process 2 must start first)...\n",
               MQ_ALERT_PACKETS);
        sleep(1);
    }
    printf("[P1] Opened %s\n", MQ_ALERT_PACKETS);

    printf("PI2-P1 NETWORK RECEIVER STARTING\n");

    pthread_t tcp_t, alert_fwd_t;
    pthread_create(&alert_fwd_t, NULL, alert_fwd_thread, NULL);
    pthread_create(&tcp_t,       NULL, tcp_rx_thread,    NULL);

    while (1) {
        printf("[P1] LINK=%d\n", atomic_load(&link_active));
        sleep(1);
    }
    return 0;
}
