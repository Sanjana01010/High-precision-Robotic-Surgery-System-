/* ================================================================ */
/* QNX  —  Pi1  PROCESS 2 :  NETWORK & HAPTICS                     */
/* Raspberry Pi 1 — Doctor Side                                     */
/*                                                                  */
/* Threads:                                                         */
/*   TCP Thread      (PRIO 50) — reads /mq/control_packets, sends   */
/*                               ControlPackets to Pi2 at 100 Hz    */
/*   Alert RX Thread (PRIO 55) — receives AlertPackets from Pi2,    */
/*                               sets motor_pending flag            */
/*   Motor Thread    (PRIO 45) — drives BCM18 vibration motor       */
/*                               with configurable pulse pattern     */
/*                                                                  */
/* IPC input:                                                        */
/*   POSIX Message Queue  /mq/control_packets  (from Process 1)     */
/*                                                                  */
/* GPIO output:                                                      */
/*   BCM18  — vibration motor (via transistor / MOSFET)             */
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
#include <math.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <mqueue.h>
#include <sys/neutrino.h>

#ifndef htobe64
  #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    #define htobe64(x) __builtin_bswap64(x)
  #else
    #define htobe64(x) (x)
  #endif
#endif

/* ================================================================ */
/* CONFIG                                                           */
/* ================================================================ */
#define SERVER_IP           "192.168.10.2"
#define SERVER_PORT         5000

/* POSIX MQ consumed from Process 1 */
#define MQ_CONTROL_PACKETS  "/mq/control_packets"

/* Vibration motor */
#define MOTOR_GPIO_PIN      18
#define MOTOR_ON_MS         200
#define MOTOR_OFF_MS        100
#define MOTOR_PULSE_COUNT     3

#define TCP_THREAD_PRIO     50
#define ALERT_THREAD_PRIO   55
#define MOTOR_THREAD_PRIO   45

/* ================================================================ */
/* ALERT PACKET  (Pi2 → Pi1, must match pi2_process1_network.c)    */
/* ================================================================ */
#define ALERT_MAGIC   0xDEADC0DEu

#define ALERT_S1_MAX  1u
#define ALERT_S1_MIN  2u
#define ALERT_S2_MAX  3u
#define ALERT_S2_MIN  4u
#define ALERT_S4_MAX  5u
#define ALERT_S4_MIN  6u

typedef struct __attribute__((packed))
{
    uint32_t magic;
    uint8_t  alert_id;
    uint8_t  _pad[3];
} AlertPacket;

/* ================================================================ */
/* CONTROL PACKET  (Pi1 → Pi2, must match pi2_process1_network.c)  */
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
/* IPC MESSAGE  (must match pi1_process1_input_control.c)           */
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
} ControlMqMsg;

/* ================================================================ */
/* SHARED STATE                                                     */
/* ================================================================ */
/*
 * motor_pending:
 *   Set to 1 by alert_rx_thread each time an AlertPacket arrives.
 *   Cleared to 0 by motor_thread after the full pulse pattern.
 *   If a second alert arrives while motor is running, the flag
 *   stays 1 and motor_thread re-runs the pattern immediately.
 */
static _Atomic int motor_pending = 0;

/* MQ handle — opened in main, consumed by tcp_thread */
static mqd_t mq_in = (mqd_t)-1;

/* ================================================================ */
/* HELPERS                                                          */
/* ================================================================ */
static void set_fifo_priority(int prio)
{
    struct sched_param sp;
    memset(&sp, 0, sizeof(sp));
    sp.sched_priority = prio;
    sched_setscheduler(0, SCHED_FIFO, &sp);
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

static const char *alert_name(uint8_t id)
{
    switch (id) {
        case ALERT_S1_MAX: return "SERVO 1  MAX LIMIT REACHED";
        case ALERT_S1_MIN: return "SERVO 1  MIN LIMIT REACHED";
        case ALERT_S2_MAX: return "SERVO 2  MAX LIMIT REACHED";
        case ALERT_S2_MIN: return "SERVO 2  MIN LIMIT REACHED";
        case ALERT_S4_MAX: return "SERVO 4  MAX LIMIT REACHED";
        case ALERT_S4_MIN: return "SERVO 4  MIN LIMIT REACHED";
        default:           return "UNKNOWN LIMIT ALERT";
    }
}

/* ================================================================ */
/* VIBRATION MOTOR                                                  */
/* ================================================================ */
static void motor_init(void)
{
    char cmd[64];
    snprintf(cmd, sizeof(cmd), "gpio-bcm2711 set %d op dl", MOTOR_GPIO_PIN);
    system(cmd);
    printf("[P2] MOTOR: GPIO BCM%d initialised (output LOW)\n", MOTOR_GPIO_PIN);
}

static void motor_on(void)
{
    char cmd[64];
    snprintf(cmd, sizeof(cmd), "gpio-bcm2711 set %d dh", MOTOR_GPIO_PIN);
    system(cmd);
}

static void motor_off(void)
{
    char cmd[64];
    snprintf(cmd, sizeof(cmd), "gpio-bcm2711 set %d dl", MOTOR_GPIO_PIN);
    system(cmd);
}

static void motor_pulse_pattern(void)
{
    for (int i = 0; i < MOTOR_PULSE_COUNT; i++)
    {
        motor_on();
        usleep((useconds_t)MOTOR_ON_MS * 1000);
        motor_off();
        if (i < MOTOR_PULSE_COUNT - 1)
            usleep((useconds_t)MOTOR_OFF_MS * 1000);
    }
}

/* ================================================================ */
/* MOTOR THREAD  (PRIO 45)                                          */
/* Polls motor_pending at 100 Hz. When set, runs pulse pattern then */
/* clears the flag. Isolated here so usleep() never touches TCP.   */
/* ================================================================ */
static void *motor_thread(void *arg)
{
    (void)arg;
    set_fifo_priority(MOTOR_THREAD_PRIO);
    motor_init();

    while (1)
    {
        if (atomic_load(&motor_pending))
        {
            motor_pulse_pattern();
            atomic_store(&motor_pending, 0);
        }
        usleep(10000);   /* 100 Hz poll */
    }
    return NULL;
}

/* ================================================================ */
/* ALERT RX THREAD  (PRIO 55)                                       */
/* Spawned by tcp_thread for each connection. Blocks on recv_all()  */
/* waiting for 8-byte AlertPackets from Pi2 on the shared socket.  */
/*   1. Validate magic.                                             */
/*   2. Print bordered warning.                                     */
/*   3. Set motor_pending = 1.                                      */
/* Exits when socket closes.                                        */
/* ================================================================ */
static void *alert_rx_thread(void *arg)
{
    int fd = (int)(intptr_t)arg;
    set_fifo_priority(ALERT_THREAD_PRIO);

    printf("[P2-ALERT RX] Ready — listening for limit alerts from Pi2\n");

    while (1)
    {
        AlertPacket ap;
        if (recv_all(fd, &ap, sizeof(ap)) < 0)
        {
            printf("[P2-ALERT RX] Socket closed — exiting\n");
            return NULL;
        }

        uint32_t magic = ntohl(ap.magic);
        if (magic != ALERT_MAGIC)
        {
            printf("[P2-ALERT RX] Unexpected data (magic=0x%08X) — ignored\n",
                   magic);
            continue;
        }

        printf("\n"
               "############################################\n"
               "## [PI2 ALERT]  %-26s##\n"
               "############################################\n\n",
               alert_name(ap.alert_id));
        fflush(stdout);

        atomic_store(&motor_pending, 1);
    }
    return NULL;
}

/* ================================================================ */
/* TCP THREAD  (PRIO 50)                                            */
/*                                                                  */
/* Connection lifecycle:                                            */
/*   1. connect() to Pi2.                                           */
/*   2. Spawn alert_rx_thread on the same sockfd (full-duplex).     */
/*   3. Drain /mq/control_packets; forward each as ControlPacket.  */
/*   4. On send failure: close socket, join alert thread, retry.    */
/*                                                                  */
/* MQ drain strategy:                                               */
/*   Blocking mq_receive() — blocks until Process 1 publishes.     */
/*   This naturally paces the TCP send loop to ~100 Hz, matching    */
/*   the rate at which Process 1 publishes.                         */
/* ================================================================ */
static void *tcp_thread(void *arg)
{
    (void)arg;
    set_fifo_priority(TCP_THREAD_PRIO);

    while (1)   /* reconnect loop */
    {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) { sleep(1); continue; }

        int yes = 1;
        setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

        struct sockaddr_in server;
        memset(&server, 0, sizeof(server));
        server.sin_family = AF_INET;
        server.sin_port   = htons(SERVER_PORT);
        inet_pton(AF_INET, SERVER_IP, &server.sin_addr);

        printf("[P2] CONNECTING TO %s:%d ...\n", SERVER_IP, SERVER_PORT);
        if (connect(sockfd, (struct sockaddr *)&server, sizeof(server)) < 0)
        {
            perror("connect");
            close(sockfd);
            sleep(1);
            continue;
        }
        printf("[P2] CONNECTED\n");

        /* Spawn alert reader — no alert missed before first send */
        pthread_t alert_t;
        pthread_create(&alert_t, NULL, alert_rx_thread,
                       (void *)(intptr_t)sockfd);

        /* ── Forward loop ── */
        while (1)
        {
            ControlMqMsg mq_msg;
            ssize_t r = mq_receive(mq_in,
                                   (char *)&mq_msg, sizeof(mq_msg),
                                   NULL);
            if (r < 0)
            {
                perror("[P2] mq_receive");
                sleep(1);
                continue;
            }

            /* Repack into wire-format ControlPacket */
            ControlPacket pkt;
            memset(&pkt, 0, sizeof(pkt));
            pkt.sequence     = htonl(mq_msg.sequence);
            pkt.timestamp_ns = htobe64(mq_msg.timestamp_ns);
            pkt.servo1_deg   = mq_msg.servo1_deg;
            pkt.servo2_deg   = mq_msg.servo2_deg;
            pkt.servo4_deg   = mq_msg.servo4_deg;
            pkt.gripper      = mq_msg.gripper;

            if (send_all(sockfd, &pkt, sizeof(pkt)) < 0)
            {
                printf("[P2] SEND FAILED — DISCONNECTED\n");
                break;
            }
        }

        /* close() unblocks recv_all() in alert_rx_thread → clean exit */
        close(sockfd);
        pthread_join(alert_t, NULL);

        printf("[P2] RECONNECTING IN 1 s...\n");
        sleep(1);
    }
    return NULL;
}

/* ================================================================ */
/* MAIN                                                             */
/* ================================================================ */
int main(void)
{
    mlockall(MCL_CURRENT | MCL_FUTURE);
    ThreadCtl(_NTO_TCTL_IO, NULL);

    /* ── Open POSIX MQ (input from Process 1) — blocking reads ── */
    struct mq_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.mq_flags   = 0;   /* blocking */
    attr.mq_maxmsg  = 8;
    attr.mq_msgsize = sizeof(ControlMqMsg);

    /*
     * Process 1 creates the queue; Process 2 opens it read-only.
     * If Process 1 hasn't started yet, retry until it does.
     */
    while ((mq_in = mq_open(MQ_CONTROL_PACKETS, O_RDONLY)) == (mqd_t)-1)
    {
        printf("[P2] Waiting for %s to be created by Process 1...\n",
               MQ_CONTROL_PACKETS);
        sleep(1);
    }
    printf("[P2] Opened %s\n", MQ_CONTROL_PACKETS);

    printf("PI1-P2 NETWORK & HAPTICS STARTING\n");
    printf("MOTOR: GPIO BCM%d  |  %d pulses x %d ms per alert\n",
           MOTOR_GPIO_PIN, MOTOR_PULSE_COUNT, MOTOR_ON_MS);

    pthread_t tcp_t, motor_t;
    pthread_create(&motor_t, NULL, motor_thread, NULL);
    pthread_create(&tcp_t,   NULL, tcp_thread,   NULL);

    /* Keep main alive */
    while (1) sleep(10);

    return 0;
}
