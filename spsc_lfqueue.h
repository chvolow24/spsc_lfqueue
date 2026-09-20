#ifndef SPSC_LFQUEUE_H
#define SPSC_LFQUEUE_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

enum {
    LFQUEUE_SUCCESS,
    LFQUEUE_FULL,
    LFQUEUE_EMPTY,
    LFQUEUE_OVERSIZE_INBUF,
    LFQUEUE_OP_CANCELED,
    LFQUEUE_LEN_LOCKED,
    LFQUEUE_NUM_STATUS_CODES
};

typedef struct lock_free_queue {
    uint8_t *buf;
    size_t el_size;
    int max_len;
    int len;
    int wrap_mask;
    _Atomic int write_i;
    _Atomic int read_i;
    _Atomic int len_lock;
} LFQueue;

/* Allocate the ring buffer and initialize values. */
int lfqueue_init(LFQueue *q, size_t el_size, int len);

/* Free the ring buffer */
void lfqueue_deinit(LFQueue *q);

/* len must be less than or equal to the initialized size.
   else, deinit and reinit with the new len
 */
int lfqueue_set_len(LFQueue *q, int len);


/* Return LFQUEUE_SUCCESS or an error code < 0 */
int lfqueue_try_enqueue(LFQueue *q, void *restrict inbuf, int inbuf_len);

/* Return LFQUEUE_SUCCESS or an error code < 0 */
int lfqueue_try_dequeue(LFQueue *q, void *restrict dstbuf, int dstbuf_len);

const char *lfqueue_get_errstr(int error_code);

/*
  Wait until space available, then enqueue and exit.
   
  When 'loop_sleep' is 0, waiting is busywaiting; else each
  iteration sleep 'loop_sleep' microseconds.
  
  If 'cancel_opt' provided, give up when *cancel_opt == true
*/
int lfqueue_wait_enqueue(
    LFQueue *q,
    void *restrict inbuf,
    int inbuf_len,
    uint64_t loop_sleep,
    uint64_t timeout_after,
    _Atomic bool *cancel_opt);

/*
  Wait data available, then dequeue and exit.
   
  When 'loop_sleep' is 0, waiting is busywaiting; else each
  iteration sleep 'loop_sleep' microseconds.
  
  If 'cancel_opt' provided, give up when *cancel_opt == true
*/
int lfqueue_wait_dequeue(
    LFQueue *q,
    void *restrict dstbuf,
    int dstbuf_len,
    uint64_t loop_sleep,
    uint64_t timeout_after,
    _Atomic bool *cancel_opt);

#ifdef SPSC_LFQUEUE_IMPL

#include <math.h>
#include <stdlib.h>
#include <time.h>

static inline int sleep_us(uint64_t us)
{
    struct timespec ts = {
        .tv_sec = us / 1000000,
        .tv_nsec = (us % 1000000) * 1000
    };
    return nanosleep(&ts, NULL);
}

static const char *lfqueue_errstr[] = {
    "Success",
    "Queue full",
    "Queue empty",
    "Cannot enqueue data longer than half ring buf len",
    "Wait canceled",
    "Queue length being reset"
};

static int reader_try_acquire_len_lock(LFQueue *q)
{
    int len_lock_val;
    do {
        len_lock_val = atomic_load_explicit(&q->len_lock, memory_order_relaxed);
        if (len_lock_val < 0) {
            return LFQUEUE_LEN_LOCKED;
        }
    } while (!atomic_compare_exchange_strong_explicit(
                 &q->len_lock,
                 &len_lock_val,
                 len_lock_val + 1,
                 memory_order_acquire,
                 memory_order_relaxed));
    return LFQUEUE_SUCCESS;
}

static void reader_release_len_lock(LFQueue *q)
{
    atomic_fetch_sub_explicit(&q->len_lock, 1, memory_order_release);
}

static void writer_wait_acquire_len_lock(LFQueue *q)
{
    int expected = 0;
    while (!atomic_compare_exchange_strong_explicit(
               &q->len_lock,
               &expected,
               -1,
               memory_order_release,
               memory_order_relaxed)) {
        expected = 0;
    }
}

static void writer_release_len_lock(LFQueue *q)
{
    atomic_store_explicit(&q->len_lock, 0, memory_order_release);
}

const char *lfqueue_get_errstr(int error_code)
{
    if (error_code < LFQUEUE_NUM_STATUS_CODES) {
        return lfqueue_errstr[error_code];
    } else {
        return "Unknown error";
    }
}

int lfqueue_init(LFQueue *q, size_t el_size, int len)
{
    if (len < 4) len = 4;
    double lg = log2(len);
    double lgfl = floor(lg);
    if (lg != lgfl) {
        len = pow(2.0, lgfl + 1.0);
    }
    atomic_store(&q->read_i, 0);
    atomic_store(&q->write_i, 0);
    q->buf = malloc(el_size * len);
    q->el_size = el_size;
    q->max_len = len;
    q->len = len;
    q->wrap_mask = len - 1;
    return len;
}

void lfqueue_deinit(LFQueue *q)
{
    if (q->buf) free(q->buf);
    q->buf = NULL;
}

int lfqueue_set_len(LFQueue *q, int len)
{
    if (len > q->max_len) return q->len;
    
    writer_wait_acquire_len_lock(q);
    
    if (len < 4) len = 4;
    double lg = log2(len);
    double lgfl = floor(lg);
    if (lg != lgfl) {
        len = pow(2.0, lgfl + 1.0);
    }
    q->len = len;
    q->wrap_mask = len - 1;
    atomic_store_explicit(&q->write_i, 0, memory_order_relaxed);
    atomic_store_explicit(&q->read_i, 0, memory_order_relaxed);
    memset(q->buf, 0, len * q->el_size);
    

    writer_release_len_lock(q);
    
    return len;
}

int lfqueue_try_enqueue(LFQueue *q, void *restrict inbuf_v, int inbuf_len)
{
    uint8_t *inbuf = inbuf_v;    
    if (inbuf_len > q->len / 2) {
        return LFQUEUE_OVERSIZE_INBUF;
    } else if (inbuf_len == 0) {
        return LFQUEUE_SUCCESS;
    }
    
    if (reader_try_acquire_len_lock(q) != LFQUEUE_SUCCESS) {
        return LFQUEUE_LEN_LOCKED;
    }
    
    int loc_write_i = atomic_load_explicit(&q->write_i, memory_order_relaxed);

    /* Load of read_i must be 'acquire' to prevent writes below from moving
       before the reader releases its index */
    int loc_read_i = atomic_load_explicit(&q->read_i, memory_order_acquire);

    /* Although the reader may read during this operation, we can guarantee
       that the index values here represent the minimum amount of writable
       space in the buffer */

    int avail_to_write =
        loc_write_i < loc_read_i ?
        loc_read_i - loc_write_i - 1 :
        (q->len - loc_write_i) + loc_read_i - 1;
    
    if (avail_to_write < inbuf_len) {
        reader_release_len_lock(q);
        return LFQUEUE_FULL;
    }
     
    int write_dst = (loc_write_i + inbuf_len) & q->wrap_mask;

    if (loc_write_i < write_dst) {
        memcpy(q->buf + loc_write_i * q->el_size, inbuf, inbuf_len * q->el_size);
    } else {
        int left = q->len - loc_write_i;
        memcpy(q->buf + loc_write_i * q->el_size, inbuf, left * q->el_size);
        memcpy(q->buf, inbuf + left * q->el_size, write_dst * q->el_size);
    }
    atomic_store_explicit(&q->write_i, write_dst, memory_order_release);

    reader_release_len_lock(q);
    
    return LFQUEUE_SUCCESS;
}

int lfqueue_try_dequeue(LFQueue *q, void *restrict dstbuf_v, int dstbuf_len)
{
    uint8_t *dstbuf = dstbuf_v;
    if (dstbuf_len == 0) return LFQUEUE_SUCCESS;

    if (reader_try_acquire_len_lock(q) != LFQUEUE_SUCCESS) {
        return LFQUEUE_LEN_LOCKED;
    }

    
    int loc_read_i = atomic_load_explicit(&q->read_i, memory_order_relaxed);
    int loc_write_i = atomic_load_explicit(&q->write_i, memory_order_acquire);

    int avail_to_read =
        loc_read_i <= loc_write_i ?
        loc_write_i - loc_read_i :
        (q->len - loc_read_i) + loc_write_i;

    if (avail_to_read < dstbuf_len) {
        reader_release_len_lock(q);
        return LFQUEUE_EMPTY;
    }

    int read_dst = (loc_read_i + dstbuf_len) & q->wrap_mask;
    if (loc_read_i < read_dst) {
        memcpy(dstbuf, q->buf + loc_read_i * q->el_size, dstbuf_len * q->el_size);
    } else {
        int left = q->len - loc_read_i;
        memcpy(dstbuf, q->buf + loc_read_i * q->el_size, left * q->el_size);
        memcpy(dstbuf + left * q->el_size, q->buf, read_dst * q->el_size);
    }
    atomic_store_explicit(&q->read_i, read_dst, memory_order_release);

    reader_release_len_lock(q);
    
    return LFQUEUE_SUCCESS;
}

int lfqueue_wait_enqueue(
    LFQueue *q,
    void *restrict inbuf_v,
    int inbuf_len,
    uint64_t loop_sleep,
    uint64_t timeout_after,
    _Atomic bool *cancel_opt)
{
    uint8_t *inbuf = inbuf_v;
    if (inbuf_len > q->len / 2) {
        return LFQUEUE_OVERSIZE_INBUF;
    } else if (inbuf_len == 0) {
        return LFQUEUE_SUCCESS;
    }

    if (reader_try_acquire_len_lock(q) != LFQUEUE_SUCCESS) {
        return LFQUEUE_LEN_LOCKED;
    }

    
    int loc_write_i;
    int loc_read_i;
    int avail_to_write;
    uint64_t accum_sleep = 0;
    while (1) {
        if (timeout_after > 0 && accum_sleep >= timeout_after) {
            goto canceled;
        }
        if (cancel_opt && atomic_load_explicit(cancel_opt, memory_order_relaxed)) {
            goto canceled;
        }
        loc_write_i = atomic_load_explicit(&q->write_i, memory_order_relaxed);

        /* Load of read_i must be 'acquire' to prevent writes below from moving
           before the reader releases its index */
        loc_read_i = atomic_load_explicit(&q->read_i, memory_order_acquire);

        /* Although the reader may read during this operation, we can guarantee
           that the index values here represent the minimum amount of writable
           space in the buffer */
        avail_to_write =
            loc_write_i < loc_read_i ?
            loc_read_i - loc_write_i - 1 :
            (q->len - loc_write_i) + loc_read_i - 1;
    
        if (avail_to_write < inbuf_len) {
            accum_sleep += loop_sleep;            
            sleep_us(loop_sleep);
            continue;
        } else {
            break;
        }
    }
     
    int write_dst = (loc_write_i + inbuf_len) & q->wrap_mask;

    if (loc_write_i < write_dst) {
        memcpy(q->buf + loc_write_i * q->el_size, inbuf, inbuf_len * q->el_size);
    } else {
        int left = q->len - loc_write_i;
        memcpy(q->buf + loc_write_i * q->el_size, inbuf, left * q->el_size);
        memcpy(q->buf, inbuf + left * q->el_size, write_dst * q->el_size);
    }
    atomic_store_explicit(&q->write_i, write_dst, memory_order_release);
    reader_release_len_lock(q);
    return LFQUEUE_SUCCESS;
canceled:
    reader_release_len_lock(q);
    return LFQUEUE_OP_CANCELED;
}

int lfqueue_wait_dequeue(
    LFQueue *q,
    void *restrict dstbuf_v,
    int dstbuf_len,
    uint64_t loop_sleep,
    uint64_t timeout_after,
    _Atomic bool *cancel_opt)
{
    uint8_t *dstbuf = dstbuf_v;
    if (dstbuf_len == 0) return LFQUEUE_SUCCESS;

    if (reader_try_acquire_len_lock(q) != LFQUEUE_SUCCESS) {
        return LFQUEUE_LEN_LOCKED;
    }

        
    int loc_read_i;
    int loc_write_i;
    int avail_to_read;
    uint64_t accum_sleep = 0;
    while (1) {
        if (timeout_after > 0 && accum_sleep >= timeout_after) {
            goto canceled;
        }
        if (cancel_opt && atomic_load_explicit(cancel_opt, memory_order_relaxed)) {
            goto canceled;
        }
        loc_read_i = atomic_load_explicit(&q->read_i, memory_order_relaxed);
        loc_write_i = atomic_load_explicit(&q->write_i, memory_order_acquire);

        avail_to_read =
            loc_read_i <= loc_write_i ?
            loc_write_i - loc_read_i :
            (q->len - loc_read_i) + loc_write_i;
        if (avail_to_read < dstbuf_len) {
            accum_sleep += loop_sleep;
            sleep_us(loop_sleep);
            continue;
        } else {
            break;
        }
    }

    int read_dst = (loc_read_i + dstbuf_len) & q->wrap_mask;
    if (loc_read_i < read_dst) {
        memcpy(dstbuf, q->buf + loc_read_i * q->el_size, dstbuf_len * q->el_size);
    } else {
        int left = q->len - loc_read_i;
        memcpy(dstbuf, q->buf + loc_read_i * q->el_size, left * q->el_size);
        memcpy(dstbuf + left * q->el_size, q->buf, read_dst * q->el_size);
    }
    atomic_store_explicit(&q->read_i, read_dst, memory_order_release);
    reader_release_len_lock(q);
    return LFQUEUE_SUCCESS;
canceled:
    reader_release_len_lock(q);
    return LFQUEUE_OP_CANCELED;
}

#endif
#endif
