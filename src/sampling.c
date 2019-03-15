#include <float.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include "sampling.h"
#include "hashmap.h"
#include "stats.h"
#include "log.h"

#ifdef __APPLE__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

#define HM_SIZE 32768

typedef struct expiring_entry {
    int hm_expiry_frequency;
    int hm_ttl;
    ev_timer map_expiry_timer; //timer to clear out expired elements from map
} expiring_entry_t;

struct sampler {
    unsigned int threshold;
    int window;
    int cardinality;
    int reservoir_size;
    bool timer_flush_min_max;
#ifdef __APPLE__
    unsigned short randbuf[3];
#else
    struct drand48_data randbuf;
#endif
    hashmap *map;
    expiring_entry_t base;
};

struct sampler_flush_data {
    sampler_t* sampler;
    void* data;
    sampler_flush_cb *cb;
};

struct sample_bucket {
    bool sampling;
    /**
     * A record of the number of events received
     */
    uint64_t last_window_count;

    /**
     * Unix timestamp of bucket last modification
     */
    time_t last_modified_at;

    /**
     * Accumulated sum
     */
    double sum;

    /**
     * Accumulated count (which may differ from last_window_count due to sampling)
     */
    uint64_t count;

    /**
     * Metric type (COUNTER, TIMER, GAUGE etc.)
     */
    metric_type type;

    /**
     * Index of recent item in the reservoir
     */
    int reservoir_index;

    /**
     * Upper value of timer seen in the sampling period
     */
    double upper;

    /**
     * Lower value of timer seen in the sampling period
     */
    double lower;

    /**
     * retain the incoming pre applied sample rate to relay to statsite
     * for the current_min
     */
    double lower_sample_rate;

    /**
     * retain the incoming pre applied sample rate to relay to statsite
     * for the current_max
     */
    double upper_sample_rate;

    /**
     * Maintain a reservoir of 'threshold' timer values
     */
    double reservoir[];
};

/**
 * Boolean flag that sampler flush callback uses to decide
 * if the calculated true upper and lower values for a sampled
 * timer needs flushing
 */
static bool flush_upper_lower(const sampler_t* sampler) {
    return sampler->timer_flush_min_max;
}

static time_t timestamp() {
    time_t timestamp_sec;
#ifdef __APPLE__
    clock_serv_t cclock;
    mach_timespec_t mts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &mts);
    mach_port_deallocate(mach_task_self(), cclock);
    timestamp_sec = mts.tv_sec;
#else
    struct timespec current_time;
    clock_gettime(CLOCK_REALTIME_COARSE, &current_time);
    timestamp_sec = current_time.tv_sec;
#endif

    return timestamp_sec;
}

static const char *metric_type_name(metric_type type) {
    switch (type) {
    case METRIC_COUNTER:
        return "counter";
    case METRIC_TIMER:
        return "timer";
    case METRIC_GAUGE:
        return "gauge";
    case METRIC_UNKNOWN:
    case METRIC_KV:
    case METRIC_HIST:
    case METRIC_S:
    default:
        return "unknown/other";
    }
}

static int sampler_update_callback(void* _s, const char* key, void* _value, void *metadata) {
    (void)metadata; // unused
    sampler_t* sampler = (sampler_t*)_s;
    struct sample_bucket* bucket = (struct sample_bucket*)_value;

    if (bucket->last_window_count > sampler->threshold) {
        bucket->sampling = true;
    } else if (bucket->sampling && bucket->last_window_count <= sampler->threshold) {
        bucket->sampling = false;
        bucket->reservoir_index = 0;
        stats_debug_log("stopped %s sampling '%s'", metric_type_name(bucket->type), key);
    }

    bucket->last_window_count = 0;
    return 0;
}

// hashmap_iter callback for removing stale entries
static int expiry_callback(void* data, const char* key, void* value, void *metadata) {
    (void)data; // unused
    (void)key;  // unused

    const struct sample_bucket* bucket = (struct sample_bucket*)value;
    const int ttl = *(int*)metadata;

    // simply return if the bucket is being sampled!
    if (bucket->sampling) {
        return HASHMAP_ITER_CONTINUE;
    }

    time_t now = timestamp();

    if ((now - bucket->last_modified_at) > ttl) {
        if (value) {
            free(value);
        }
        return HASHMAP_ITER_DELETE;
    }

    return HASHMAP_ITER_CONTINUE;
}

#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif

static int check_sampler_snprintf(const int ret, const int bufsz)  {
    if (unlikely(ret < 0 || ret >= bufsz)) {
        if (ret < 0) {
            stats_error_log("sampling: snprintf encoding error: %d", ret);
        } else if (ret == bufsz) {
            stats_error_log("sampling: snprintf buffer too small: %d", ret);
        } else {
            // This should not happen, unless we pass a NULL buffer to get
            // the required bufsz in which case this function should have
            // been called.
            stats_error_log("sampling: snprintf invalid return value: %d", ret);
        }
        return 1;
    }
    return 0;
}

static int sampler_flush_callback(void* _s, const char* key, void* _value, void* metadata) {

    struct sampler_flush_data* flush_data = (struct sampler_flush_data*)_s;
    struct sample_bucket* bucket = (struct sample_bucket*)_value;
    sampler_t* sampler = (sampler_t*)flush_data->sampler;

    int len;
    static char line_buffer[MAX_UDP_LENGTH];

    if (!bucket->sampling || bucket->count == 0) {
        goto exit;
    }

    switch (bucket->type) {
    case METRIC_COUNTER:
        len = snprintf(line_buffer, MAX_UDP_LENGTH, "%s:%g|c@%g\n",
            key, bucket->sum / bucket->count, 1.0 / bucket->count);
        if (check_sampler_snprintf(len, MAX_UDP_LENGTH)) {
            goto exit;
        }
        len -= 1; /* \n is not part of the length */
        flush_data->cb(flush_data->data, key, line_buffer, len);
        break;

    case METRIC_GAUGE:
        len = snprintf(line_buffer, MAX_UDP_LENGTH, "%s:%g|g\n",
            key, bucket->sum / bucket->count);
        if (check_sampler_snprintf(len, MAX_UDP_LENGTH)) {
            goto exit;
        }
        len -= 1; /* \n is not part of the length */
        flush_data->cb(flush_data->data, key, line_buffer, len);
        break;

    case METRIC_TIMER:
        // Flush the max and min for the well-being of timer.upper and
        // timer.lower respectively if, client has explicitly requested
        // a flush of .upper and .lower
        if (flush_upper_lower(sampler)) {
            if (bucket->upper > DBL_MIN) {
                len = snprintf(line_buffer, MAX_UDP_LENGTH, "%s:%g|ms@%g\n",
                    key, bucket->upper, bucket->upper_sample_rate);
                if (check_sampler_snprintf(len, MAX_UDP_LENGTH)) {
                    goto exit;
                }
                len -= 1;
                flush_data->cb(flush_data->data, key, line_buffer, len);
                bucket->upper = DBL_MIN;
            }
            if (bucket->lower < DBL_MAX) {
                len = snprintf(line_buffer, MAX_UDP_LENGTH, "%s:%g|ms@%g\n",
                    key, bucket->lower, bucket->lower_sample_rate);
                if (check_sampler_snprintf(len, MAX_UDP_LENGTH)) {
                    goto exit;
                }
                len -= 1;
                flush_data->cb(flush_data->data, key, line_buffer, len);
                bucket->lower = DBL_MAX;
            }
        }

        int num_samples = 0;
        const int threshold = sampler->threshold;
        for (int j = 0; j < threshold; j++) {
            if (!isnan(bucket->reservoir[j])) {
                num_samples++;
            }
        }
        const double sample_rate = (double)num_samples / (double)bucket->count;

        for (int j = 0; j < threshold; j++) {
            if (!isnan(bucket->reservoir[j])) {
                len = snprintf(line_buffer, MAX_UDP_LENGTH, "%s:%g|ms@%g\n",
                    key, bucket->reservoir[j], sample_rate);
                if (check_sampler_snprintf(len, MAX_UDP_LENGTH)) {
                    goto exit;
                }
                len -= 1;
                flush_data->cb(flush_data->data, key, line_buffer, len);
                bucket->reservoir[j] = NAN;
            }
        }
        break;

    case METRIC_UNKNOWN:
    case METRIC_KV:
    case METRIC_HIST:
    case METRIC_S:
        break; // do nothing
    }

    bucket->count = 0;
    bucket->sum = 0;

exit:
    /* Also call update */
    sampler_update_callback(sampler, key, _value, metadata);
    return HASHMAP_ITER_CONTINUE;
}

/**
 * Decides whether the metric should be flagged as being over
 * cardinality
 */
static bool flag_incoming_metric(sampler_t* sampler) {
    return hashmap_size(sampler->map) >= sampler->cardinality;
}

static void expiry_callback_handler(struct ev_loop *loop, struct ev_timer *timer, int events) {
    (void)events; // unused
    sampler_t* sampler = (sampler_t*)timer->data;

    // Iterate over items and callback.
    hashmap_iter(sampler->map, expiry_callback, (void *)timer->data);
    ev_timer_set(&sampler->base.map_expiry_timer, sampler->base.hm_expiry_frequency, 0.0);
    ev_timer_start(loop, &sampler->base.map_expiry_timer);
}

// sampler_init initializes the sampler, if there is an error 1 is returned.
int sampler_init(sampler_t** sampler, int threshold, int window, int cardinality, int reservoir_size,
                 bool timer_flush_min_max, int hm_expiry_frequency, int hm_ttl) {

    if (threshold < 0) {
        return 1;
    }
    struct sampler *sam = calloc(1, sizeof(struct sampler));

    hashmap_init(HM_SIZE, &sam->map);

    sam->threshold = threshold;
    sam->window = window;
    sam->cardinality = cardinality;
    sam->reservoir_size = reservoir_size;
    sam->timer_flush_min_max = timer_flush_min_max;
    sam->base.hm_expiry_frequency = hm_expiry_frequency;
    // save this to pass into hashmap_put
    sam->base.hm_ttl = hm_ttl;

    if (hm_ttl != -1) {
        struct ev_loop *loop = ev_default_loop(0);
        ev_timer_init(&sam->base.map_expiry_timer, expiry_callback_handler, sam->base.hm_expiry_frequency, 0);
        sam->base.map_expiry_timer.data = (void*)sam;
        ev_timer_start(loop, &sam->base.map_expiry_timer);
    }

#ifdef __APPLE__
    time_t t = time(NULL);
    sam->randbuf[0] = t & 0xFFFF;
    sam->randbuf[1] = (t >> 16) & 0xFFFF;
    sam->randbuf[2] = (t >> 32) & 0xFFFF;
#else
    srand48_r(time(NULL), &sam->randbuf);
#endif

    *sampler = sam;
    return 0;
}

void sampler_flush(sampler_t* sampler, sampler_flush_cb cb, void* data) {
    struct sampler_flush_data fd = {
            .data = data,
            .sampler = sampler,
            .cb = cb
    };
    hashmap_iter(sampler->map, sampler_flush_callback, (void*)&fd);
}

sampling_result sampler_is_sampling(sampler_t* sampler, const char* name, metric_type type) {
    struct sample_bucket* bucket;
    if (hashmap_get(sampler->map, name, (void**)&bucket) != 0) {
        return SAMPLER_NOT_SAMPLING;
    }
    if (bucket->sampling && bucket->type == type) {
        return SAMPLER_SAMPLING;
    }
    return SAMPLER_NOT_SAMPLING;
}

void sampler_update_flags(sampler_t* sampler) {
    hashmap_iter(sampler->map, sampler_update_callback, (void*)sampler);
}

sampling_result sampler_consider_counter(sampler_t* sampler, const char* name, validate_parsed_result_t* parsed) {
    // safety check, also checked for in stats.c
    if (parsed->type != METRIC_COUNTER) {
        return SAMPLER_NOT_SAMPLING;
    }

    struct sample_bucket* bucket = NULL;
    hashmap_get(sampler->map, name, (void**)&bucket);
    if (bucket == NULL) {
        // Only flag if its a new metric
        if (flag_incoming_metric(sampler)) {
            stats_error_log("flagging counter: %s", name);
            return SAMPLER_FLAGGED;
        }
        /* Intialize a new bucket */
        bucket = malloc(sizeof(struct sample_bucket));
        if (bucket == NULL) {
            // Memory allocation has failed - fail by flagging metrics
            return SAMPLER_FLAGGED;
        }
        bucket->sampling = false;
        bucket->last_window_count = 1;
        bucket->type = parsed->type;
        bucket->sum = 0;
        bucket->count = 0;
        bucket->last_modified_at = timestamp();
        hashmap_put(sampler->map, name, (void*)bucket, (void*)&sampler->base.hm_ttl);
    } else {
        bucket->last_window_count++;
        bucket->last_modified_at = timestamp();

        /* Circuit break and enable sampling mode */
        if (!bucket->sampling && bucket->last_window_count > sampler->threshold) {
            stats_debug_log("started counter sampling '%s'", name);
            bucket->sampling = true;
        }

        if (bucket->sampling) {
            double value = parsed->value;
            double count = 1.0;
            if (parsed->presampling_value > 0.0 && parsed->presampling_value < 1.0) {
                value = value * (1.0 / parsed->presampling_value);
                count = 1 * (1.0 / parsed->presampling_value);
            }
            bucket->sum += value;
            bucket->count += count;

            return SAMPLER_SAMPLING;
        }
    }
    return SAMPLER_NOT_SAMPLING;
}

sampling_result sampler_consider_timer(sampler_t* sampler, const char* name, validate_parsed_result_t* parsed) {
    // safety check, also checked for in stats.c
    if (parsed->type != METRIC_TIMER) {
        return SAMPLER_NOT_SAMPLING;
    }

    struct sample_bucket* bucket = NULL;
    hashmap_get(sampler->map, name, (void**)&bucket);
    if (bucket == NULL) {
        // Only flag if its a new metric
        if (flag_incoming_metric(sampler)) {
            stats_error_log("flagging timer: %s", name);
            return SAMPLER_FLAGGED;
        }
        /* Intialize a new bucket */
        bucket = malloc(sizeof(struct sample_bucket) + (sizeof(double) * sampler->reservoir_size));
        if (bucket == NULL) {
            // Memory allocation has failed - fail by flagging metrics
            return SAMPLER_FLAGGED;
        }
        bucket->sampling = false;
        bucket->reservoir_index = 0;
        bucket->last_window_count = 0;
        bucket->type = parsed->type;
        bucket->upper = DBL_MIN;
        bucket->lower = DBL_MAX;
        bucket->sum = 0;
        bucket->count = 0;
        bucket->last_modified_at = timestamp();

        for (int k = 0; k < sampler->threshold; k++) {
            bucket->reservoir[k] = NAN;
        }
        bucket->last_window_count += 1;
        hashmap_put(sampler->map, name, (void*)bucket, (void*)&sampler->base.hm_ttl);
    } else {
        bucket->last_window_count++;
        bucket->last_modified_at = timestamp();

        /* Circuit break and enable sampling mode */
        if (!bucket->sampling && bucket->last_window_count > sampler->threshold) {
            stats_debug_log("started timer sampling '%s'", name);
            bucket->sampling = true;
        }

        if (bucket->sampling) {
            double value = parsed->value;

            /**
             * update the upper and lower
             * timer values.
             */
            if (value > bucket->upper) {
                // keep the sampling rate in sync with the value
                bucket->upper_sample_rate = parsed->presampling_value;

                if (bucket->upper != DBL_MIN) {
                    // add previous_max to reservoir
                    // update current_max
                    double old_max = bucket->upper;
                    bucket->upper = value;
                    value = old_max;
                } else {
                    // dont include it in the reservoir
                    bucket->upper = value;
                    return SAMPLER_SAMPLING;
                }
            }

            if (value < bucket->lower) {
                // keep the sampling rate in sync with the value
                bucket->lower_sample_rate = parsed->presampling_value;

                if (bucket->lower != DBL_MAX) {
                    // add previous_min to reservoir
                    // update current_min
                    double old_min = bucket->lower;
                    bucket->lower = value;
                    value = old_min;
                } else {
                    // dont include it in the reservoir
                    bucket->lower = value;
                    return SAMPLER_SAMPLING;
                }
            }

            if (bucket->reservoir_index < sampler->threshold) {
                bucket->reservoir[bucket->reservoir_index++] = value;
            } else {
                long int i, k;
#ifdef __APPLE__
                i = nrand48(sampler->randbuf);
#else
                lrand48_r(&sampler->randbuf, &i);
#endif
                k = i % (bucket->last_window_count);

                if (k < sampler->threshold) {
                    bucket->reservoir[k] = value;
                }
            }

            double count = 1.0;
            if (parsed->presampling_value > 0.0 && parsed->presampling_value < 1.0) {
                count = 1 * (1.0 / parsed->presampling_value);
            }

            bucket->sum += value;
            bucket->count += count;
            return SAMPLER_SAMPLING;
        }
    }

    return SAMPLER_NOT_SAMPLING;
}

sampling_result sampler_consider_gauge(sampler_t* sampler, const char* name, validate_parsed_result_t* parsed) {
    struct sample_bucket* bucket = NULL;

    if (parsed->type != METRIC_GAUGE) {
        return SAMPLER_NOT_SAMPLING;
    }

    hashmap_get(sampler->map, name, (void**)&bucket);
    if (bucket == NULL) {
        // Only flag if its a new metric
        if (flag_incoming_metric(sampler)) {
            stats_error_log("flagging gauge: %s", name);
            return SAMPLER_FLAGGED;
        }
        /* Intialize a new bucket */
        bucket = malloc(sizeof(struct sample_bucket));
        if (bucket == NULL) {
            // Memory allocation has failed - fail by flagging metrics
            return SAMPLER_FLAGGED;
        }
        bucket->sampling = false;
        bucket->last_window_count = 0;
        bucket->type = parsed->type;
        bucket->sum = 0;
        bucket->count = 0;
        bucket->last_modified_at = timestamp();
        hashmap_put(sampler->map, name, (void*)bucket, (void*)&sampler->base.hm_ttl);
    }

    bucket->last_modified_at = timestamp();
    if (sampler->threshold <= 0) {
        return SAMPLER_NOT_SAMPLING;
    }

    bucket->last_window_count++;

    /* Circuit break and enable sampling mode */
    if (!bucket->sampling && bucket->last_window_count > sampler->threshold) {
        stats_debug_log("started gauge sampling '%s'", name);
        bucket->sampling = true;
    }

    if (bucket->sampling) {
        double value = parsed->value;
        double count = 1.0;

        bucket->sum += value;
        bucket->count += count;

        return SAMPLER_SAMPLING;
    }

    return SAMPLER_NOT_SAMPLING;
}

int sampler_window(const sampler_t* sampler) {
    return sampler->window;
}

void sampler_destroy(sampler_t* sampler) {
    if (sampler->base.hm_ttl != -1) {
        stats_debug_log("Stopping passive hashmap expiry timer.");
        struct ev_loop* loop = ev_default_loop(0);
        ev_timer_stop(loop, &sampler->base.map_expiry_timer);
    }
    hashmap_destroy(sampler->map);
}

int sampler_expiration_timer_frequency(sampler_t* sampler) {
    return sampler->base.hm_expiry_frequency;
}

bool is_expiry_watcher_active(sampler_t* sampler) {
    return sampler->base.hm_expiry_frequency != -1 ? ev_is_active(&sampler->base.map_expiry_timer) : 0;
}

bool is_expiry_watcher_pending(sampler_t* sampler) {
    return sampler->base.hm_expiry_frequency != -1 ? ev_is_pending(&sampler->base.map_expiry_timer) : 0;
}
