/* ================================================================ */
/* QNX  —  Pi2  PROCESS 2 :  SERVO CONTROLLER                      */
/* Raspberry Pi 2 — Arm Side — PCA9685                              */
/*                                                                  */
/* Threads:                                                         */
/*   Servo Thread    (PRIO 70) — reads /mq/control_targets,         */
/*                               interpolates, drives PCA9685,      */
/*                               publishes AlertMqMsg on limit hit  */
/*   Watchdog Thread (PRIO 30) — homes servos on packet timeout     */
/*                                                                  */
/* Servo limits (confirmed by hardware test):                       */
/*   S1 : 30  – 170 deg                                             */
/*   S2 : 0   – 180 deg                                             */
/*   S4 : 0   – 120 deg                                             */
/*                                                                  */
/* IPC input:                                                        */
/*   POSIX Message Queue  /mq/control_targets  (from Process 1)     */
/*                                                                  */
/* IPC output (alert path):                                         */
/*   POSIX Message Queue  /mq/alert_packets    (to Process 1)       */
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
#include <math.h>
#include <mqueue.h>
#include <devctl.h>
#include <hw/i2c.h>
#include <sys/neutrino.h>

/* ================================================================ */
/* CONFIG                                                           */
/* ================================================================ */
#define PCA9685_ADDR        0x40
#define MIN_PULSE_TICKS     150
#define MAX_PULSE_TICKS     470
#define SERVO_PERIOD_NS     10000000ULL   /* 100 Hz servo loop     */

#define SERVO_THREAD_PRIO   70
#define WATCHDOG_PRIO       30

#define WATCHDOG_TIMEOUT_NS 500000000ULL  /* 500 ms                */

#define GRIPPER_OPEN_DEG    10.0f
#define GRIPPER_CLOSE_DEG   170.0f
#define INTERPOLATION_GAIN  0.15f

/* Physical servo limits */
#define S1_MIN   30.0f
#define S1_MAX  170.0f
#define S2_MIN    0.0f
#define S2_MAX  180.0f
#define S4_MIN    0.0f
#define S4_MAX  120.0f

/*
 * Alert fires this many degrees before the hard wall.
 * Gives Pi1 a small margin to react before the servo stalls.
 */
#define LIMIT_THRESHOLD  2.0f

/* POSIX MQs */
#define MQ_CONTROL_TARGETS  "/mq/control_targets"   /* read  ← Process 1 */
#define MQ_ALERT_PACKETS    "/mq/alert_packets"     /* write → Process 1  */
#define MQ_MAXMSG           8

/* ================================================================ */
/* ALERT IDs  (must match pi2_process1_network_receiver.c)          */
/* ================================================================ */
#define ALERT_S1_MAX  1u
#define ALERT_S1_MIN  2u
#define ALERT_S2_MAX  3u
#define ALERT_S2_MIN  4u
#define ALERT_S4_MAX  5u
#define ALERT_S4_MIN  6u

/* ================================================================ */
/* IPC MESSAGES                                                     */
/* ================================================================ */

/* Consumed from /mq/control_targets ← Process 1 */
typedef struct __attribute__((packed))
{
    float    servo1_deg;
    float    servo2_deg;
    float    servo4_deg;
    uint8_t  gripper;
    uint8_t  _pad[3];
} TargetMqMsg;

/* Published to /mq/alert_packets → Process 1 */
typedef struct __attribute__((packed))
{
    uint8_t  alert_id;
    uint8_t  _pad[3];
} AlertMqMsg;

/* ================================================================ */
/* I2C / PCA9685                                                    */
/* ================================================================ */
typedef struct { i2c_send_t hdr; uint8_t buf[32]; } i2c_msg_t;
static int i2c_fd = -1;

static void wr(uint8_t reg, uint8_t val)
{
    i2c_msg_t m;  int status;
    memset(&m, 0, sizeof(m));
    m.buf[0] = reg;  m.buf[1] = val;
    m.hdr.slave.addr = PCA9685_ADDR;
    m.hdr.slave.fmt  = I2C_ADDRFMT_7BIT;
    m.hdr.len = 2;  m.hdr.stop = 1;
    devctl(i2c_fd, DCMD_I2C_SEND, &m, sizeof(m), &status);
}

static void init_pca9685(void)
{
    wr(0x00, 0x10); usleep(5000);
    wr(0xFE, 121);  usleep(5000);
    wr(0x00, 0x00); usleep(5000);
    wr(0x00, 0xA0); usleep(5000);
}

static float clamp_angle(float a)
{
    if (a <   0.0f) a =   0.0f;
    if (a > 180.0f) a = 180.0f;
    return a;
}

static uint16_t angle_to_ticks(float a)
{
    a = clamp_angle(a);
    return (uint16_t)(MIN_PULSE_TICKS +
                      (a / 180.0f) * (MAX_PULSE_TICKS - MIN_PULSE_TICKS));
}

static void set_4_servos(float s1, float s2, float grip, float s4)
{
    i2c_msg_t m;  int status;
    memset(&m, 0, sizeof(m));
    uint16_t t0 = angle_to_ticks(s1);
    uint16_t t1 = angle_to_ticks(s2);
    uint16_t t2 = angle_to_ticks(grip);
    uint16_t t3 = angle_to_ticks(s4);

    m.buf[ 0] = 0x06;
    m.buf[ 1] = 0;          m.buf[ 2] = 0;
    m.buf[ 3] = t0 & 0xFF;  m.buf[ 4] = t0 >> 8;
    m.buf[ 5] = 0;          m.buf[ 6] = 0;
    m.buf[ 7] = t1 & 0xFF;  m.buf[ 8] = t1 >> 8;
    m.buf[ 9] = 0;          m.buf[10] = 0;
    m.buf[11] = t2 & 0xFF;  m.buf[12] = t2 >> 8;
    m.buf[13] = 0;          m.buf[14] = 0;
    m.buf[15] = t3 & 0xFF;  m.buf[16] = t3 >> 8;

    m.hdr.slave.addr = PCA9685_ADDR;
    m.hdr.slave.fmt  = I2C_ADDRFMT_7BIT;
    m.hdr.len = 17;  m.hdr.stop = 1;
    devctl(i2c_fd, DCMD_I2C_SEND, &m, sizeof(m), &status);
}

/* ================================================================ */
/* SHARED STATE                                                     */
/* ================================================================ */
static _Atomic float    target_s1        = 90.0f;
static _Atomic float    target_s2        = 90.0f;
static _Atomic float    target_s4        = 90.0f;
static _Atomic uint8_t  target_grip      = 0;
static _Atomic uint64_t last_target_ns   = 0;   /* updated per MQ msg */
static _Atomic int      link_active      = 0;

/* MQ handles */
static mqd_t mq_targets = (mqd_t)-1;   /* read  ← Process 1 */
static mqd_t mq_alerts  = (mqd_t)-1;   /* write → Process 1 */

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

/*
 * send_limit_alert_mq
 * -------------------
 * Publishes an AlertMqMsg to /mq/alert_packets.
 * Non-blocking: if the queue is full (Process 1 lagging), the alert
 * is dropped to avoid stalling the servo loop.
 */
static void send_limit_alert_mq(uint8_t alert_id)
{
    AlertMqMsg amsg;
    amsg.alert_id = alert_id;
    memset(amsg._pad, 0, sizeof(amsg._pad));

    if (mq_send(mq_alerts, (const char *)&amsg, sizeof(amsg), 0) == -1)
    {
        if (errno != EAGAIN)
            perror("[P2] mq_send alert_packets");
    }
}

/* ================================================================ */
/* SERVO THREAD  (PRIO 70)                                          */
/*                                                                  */
/* Two-stage operation per tick:                                    */
/*                                                                  */
/*  A) Drain the MQ (non-blocking) — pick up latest target.         */
/*     Keeps the servo loop at 100 Hz regardless of MQ fill rate.  */
/*                                                                  */
/*  B) Interpolate → hard-clamp → drive PCA9685.                    */
/*                                                                  */
/*  C) Limit detection with one-shot alert + auto-reset.            */
/* ================================================================ */
static void *servo_thread(void *arg)
{
    (void)arg;
    set_fifo_priority(SERVO_THREAD_PRIO);

    float cur_s1 = 90.0f;
    float cur_s2 = 90.0f;
    float cur_s4 = 90.0f;

    int s1_max_alerted = 0, s1_min_alerted = 0;
    int s2_max_alerted = 0, s2_min_alerted = 0;
    int s4_max_alerted = 0, s4_min_alerted = 0;

    struct timespec next;
    clock_gettime(CLOCK_MONOTONIC, &next);

    while (1)
    {
        /* ── A) Drain MQ (non-blocking, keep latest) ── */
        {
            TargetMqMsg tmsg;
            ssize_t r;
            while ((r = mq_receive(mq_targets,
                                   (char *)&tmsg, sizeof(tmsg),
                                   NULL)) > 0)
            {
                atomic_store(&target_s1,   tmsg.servo1_deg);
                atomic_store(&target_s2,   tmsg.servo2_deg);
                atomic_store(&target_s4,   tmsg.servo4_deg);
                atomic_store(&target_grip, tmsg.gripper);
                atomic_store(&last_target_ns, now_ns());
                atomic_store(&link_active, 1);
            }
            /* EAGAIN is normal (queue empty) — anything else is real */
            if (r < 0 && errno != EAGAIN)
                perror("[P2] mq_receive targets");
        }

        /* ── B) Interpolate ── */
        cur_s1 += INTERPOLATION_GAIN * (atomic_load(&target_s1) - cur_s1);
        cur_s2 += INTERPOLATION_GAIN * (atomic_load(&target_s2) - cur_s2);
        cur_s4 += INTERPOLATION_GAIN * (atomic_load(&target_s4) - cur_s4);

        /* ── Hard-clamp to physical limits ── */
        if (cur_s1 < S1_MIN) cur_s1 = S1_MIN;
        if (cur_s1 > S1_MAX) cur_s1 = S1_MAX;
        if (cur_s2 < S2_MIN) cur_s2 = S2_MIN;
        if (cur_s2 > S2_MAX) cur_s2 = S2_MAX;
        if (cur_s4 < S4_MIN) cur_s4 = S4_MIN;
        if (cur_s4 > S4_MAX) cur_s4 = S4_MAX;

        /* ── Drive hardware ── */
        uint8_t grip       = atomic_load(&target_grip);
        float   grip_angle = grip ? GRIPPER_CLOSE_DEG : GRIPPER_OPEN_DEG;
        set_4_servos(cur_s1, cur_s2, grip_angle, cur_s4);

        /* ── C) Limit detection (one-shot per crossing, auto-reset) ── */

/* Fires when value approaches or hits the MAX wall */
#define DETECT_MAX(val, wall, flag, id, label)              \
        if ((val) >= (wall) - LIMIT_THRESHOLD) {            \
            if (!(flag)) {                                   \
                printf("[P2] " label " MAX REACHED"         \
                       " (%.1f / %.0f deg)\n",              \
                       (double)(val), (double)(wall));       \
                send_limit_alert_mq(id);                    \
                (flag) = 1;                                  \
            }                                               \
        } else { (flag) = 0; }

/* Fires when value approaches or hits the MIN wall */
#define DETECT_MIN(val, wall, flag, id, label)              \
        if ((val) <= (wall) + LIMIT_THRESHOLD) {            \
            if (!(flag)) {                                   \
                printf("[P2] " label " MIN REACHED"         \
                       " (%.1f / %.0f deg)\n",              \
                       (double)(val), (double)(wall));       \
                send_limit_alert_mq(id);                    \
                (flag) = 1;                                  \
            }                                               \
        } else { (flag) = 0; }

        DETECT_MAX(cur_s1, S1_MAX, s1_max_alerted, ALERT_S1_MAX, "S1")
        DETECT_MIN(cur_s1, S1_MIN, s1_min_alerted, ALERT_S1_MIN, "S1")
        DETECT_MAX(cur_s2, S2_MAX, s2_max_alerted, ALERT_S2_MAX, "S2")
        DETECT_MIN(cur_s2, S2_MIN, s2_min_alerted, ALERT_S2_MIN, "S2")
        DETECT_MAX(cur_s4, S4_MAX, s4_max_alerted, ALERT_S4_MAX, "S4")
        DETECT_MIN(cur_s4, S4_MIN, s4_min_alerted, ALERT_S4_MIN, "S4")

#undef DETECT_MAX
#undef DETECT_MIN

        /* ── Next 100 Hz tick ── */
        next.tv_nsec += SERVO_PERIOD_NS;
        if (next.tv_nsec >= 1000000000LL) {
            next.tv_sec++;
            next.tv_nsec -= 1000000000LL;
        }
        clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &next, NULL);
    }
    return NULL;
}

/* ================================================================ */
/* WATCHDOG THREAD  (PRIO 30)                                       */
/* Homes all servos if no MQ message received within 500 ms.        */
/* ================================================================ */
static void *watchdog_thread(void *arg)
{
    (void)arg;
    set_fifo_priority(WATCHDOG_PRIO);

    while (1)
    {
        uint64_t last_ns = atomic_load(&last_target_ns);

        if (atomic_load(&link_active) && last_ns > 0 &&
            (now_ns() - last_ns) > WATCHDOG_TIMEOUT_NS)
        {
            printf("[P2] WATCHDOG: TIMEOUT — HOMING ALL SERVOS\n");
            atomic_store(&target_s1,   90.0f);
            atomic_store(&target_s2,   90.0f);
            atomic_store(&target_s4,   90.0f);
            atomic_store(&target_grip, 0);
            atomic_store(&link_active, 0);
        }
        usleep(50000);   /* 20 Hz check */
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

    /* ── I2C / PCA9685 ── */
    i2c_fd = open("/dev/i2c1", O_RDWR);
    if (i2c_fd < 0) { perror("i2c"); return 1; }

    init_pca9685();
    set_4_servos(90.0f, 90.0f, GRIPPER_OPEN_DEG, 90.0f);
    printf("[P2] SERVOS HOMED TO 90 DEG\n");
    printf("[P2] LIMITS  S1:[%.0f-%.0f]  S2:[%.0f-%.0f]  S4:[%.0f-%.0f]\n",
           (double)S1_MIN, (double)S1_MAX,
           (double)S2_MIN, (double)S2_MAX,
           (double)S4_MIN, (double)S4_MAX);

    /* ── Create /mq/alert_packets (Process 1 reads this) ── */
    struct mq_attr aattr;
    memset(&aattr, 0, sizeof(aattr));
    aattr.mq_flags   = O_NONBLOCK;
    aattr.mq_maxmsg  = MQ_MAXMSG;
    aattr.mq_msgsize = sizeof(AlertMqMsg);

    mq_alerts = mq_open(MQ_ALERT_PACKETS,
                        O_CREAT | O_WRONLY | O_NONBLOCK,
                        0660, &aattr);
    if (mq_alerts == (mqd_t)-1)
        { perror("mq_open alert_packets"); return 1; }

    /* ── Open /mq/control_targets (Process 1 creates this) ── */
    struct mq_attr tattr;
    memset(&tattr, 0, sizeof(tattr));
    tattr.mq_flags   = O_NONBLOCK;   /* servo thread drains non-blocking */
    tattr.mq_maxmsg  = MQ_MAXMSG;
    tattr.mq_msgsize = sizeof(TargetMqMsg);

    while ((mq_targets = mq_open(MQ_CONTROL_TARGETS,
                                 O_RDONLY | O_NONBLOCK)) == (mqd_t)-1)
    {
        printf("[P2] Waiting for %s (Process 1 must start first)...\n",
               MQ_CONTROL_TARGETS);
        sleep(1);
    }
    printf("[P2] Opened %s\n", MQ_CONTROL_TARGETS);

    printf("PI2-P2 SERVO CONTROLLER STARTING\n");

    pthread_t servo_t, wd_t;
    pthread_create(&servo_t, NULL, servo_thread,    NULL);
    pthread_create(&wd_t,    NULL, watchdog_thread, NULL);

    /* Status loop */
    while (1) {
        printf("[P2] LINK=%d\n", atomic_load(&link_active));
        sleep(1);
    }
    return 0;
}
