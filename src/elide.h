#ifndef STATSRELAY_ELIDE_H
#define STATSRELAY_ELIDE_H

#include <ev.h>
#include "hashmap.h"
#include "log.h"

typedef struct {
    /* Generations is the number of squential 0 values */
    int generations;
    struct timeval last_seen;
} elide_value_t;

typedef struct {
    hashmap *elide_map;
    int skip;
    int gc_frequency;
    int gc_ttl;
    ev_timer gc_timer;
} elide_t;

/**
 * Initialize an elision map to report constant values
 * Args:
 *  skip: generation addition. All reported generations will
 *        returned with this value added. Used to introduce jitter
 */
extern int elide_init(elide_t** e, int skip, int gc_frequency, int gc_ttl);

/**
 * Record and report on an eliding value. The return value is
 * the current generation of this elided value.
 */
extern int elide_mark(elide_t* e, char* key, struct timeval now);

/**
 * Unmark a possibly elided value, if for example its no
 * longer the last reported value. This sets generations to 0
 * which will force it to send the next time it becomes 0
 */
extern int elide_unmark(elide_t* e, char* key, struct timeval now);

/**
 * Consider replacing the elision map with a new copy.
 * Keys with are copied if the key is newer than the given
 * timestamp.
 */
extern int elide_gc(elide_t* e, struct timeval cutoff);

/**
 * Destroys the entire elision space
 */
extern int elide_destroy(elide_t* e);

#endif //STATSRELAY_ELIDE_H
