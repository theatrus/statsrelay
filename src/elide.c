#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "elide.h"


struct cb_info {
    struct timeval cutoff;
};

static int elide_gc_cb(void* data, const char *key, void* value, void *metadata) {
    struct cb_info* info = (struct cb_info*)data;
    elide_value_t* oldvalue = (elide_value_t*)value;

    /* Values not yet expired are not marked for removal */
    if (oldvalue->last_seen.tv_sec > info->cutoff.tv_sec) {
        return 0;
    }
    return 1;
}

static void elide_gc_callback_handler(struct ev_loop* loop, struct ev_timer* timer, int events) {
    elide_t* e = (elide_t*)timer->data;

    struct timeval cutoff;
    gettimeofday(&cutoff, NULL);
    cutoff.tv_sec -= e->gc_ttl;
    struct cb_info cb = {
            .cutoff = cutoff
    };
    stats_log("gc starting, hashmap tablesize=%d size=%d", hashmap_tablesize(e->elide_map), hashmap_size(e->elide_map));
    hashmap_filter(e->elide_map, elide_gc_cb, (void *) &cb);
    stats_log("gc complete, hashmap tablesize=%d size=%d", hashmap_tablesize(e->elide_map), hashmap_size(e->elide_map));

    struct ev_loop* loop = ev_default_loop(0);
    ev_timer_set(&e->gc_timer, e->gc_frequency, 0.0);
    ev_timer_start(loop, &e->gc_timer);
}

int elide_init(elide_t** e, int skip, int gc_frequency, int gc_ttl) {
    elide_t* el = malloc(sizeof(elide_t));
    int res = hashmap_init(0, &el->elide_map);
    el->skip = skip;
    el->gc_frequency = gc_frequency;
    el->gc_ttl = gc_ttl;

    if (el->gc_ttl != -1) {
        struct ev_loop* loop = ev_default_loop(0);
        ev_timer_init(&el->gc_timer, elide_gc_callback_handler, el->gc_frequency, 0);
        el->gc_timer.data = (void*)el;
        ev_timer_start(loop, &el->gc_timer);
    }

    *e = el;
    return res;
}

int elide_mark(elide_t* e, char* key, struct timeval now) {
    elide_value_t *v;
    int res = hashmap_get(e->elide_map, key, (void**)&v);
    if (res == -1) {
        v = calloc(sizeof(elide_value_t), 1);
        v->generations = e->skip;
        hashmap_put(e->elide_map, key, v, NULL);
    }
    memcpy(&v->last_seen, &now, sizeof(struct timeval));
    return v->generations++;
}

int elide_unmark(elide_t* e, char *key, struct timeval now) {
    elide_value_t* v;
    int res = hashmap_get(e->elide_map, key, (void**)&v);
    if (res == -1) {
        v = calloc(sizeof(elide_value_t), 1);
        hashmap_put(e->elide_map, key, v, NULL);
    }
    memcpy(&v->last_seen, &now, sizeof(struct timeval));
    v->generations = e->skip;
    return e->skip;
}

static int elide_delete_cb(void* data, const char *key, void* value, void *metadata) {
    free(value);
    return 0;
}

int elide_destroy(elide_t* e) {
    hashmap_iter(e->elide_map, elide_delete_cb, NULL);
    hashmap_destroy(e->elide_map);
    if (e->gc_ttl != -1) {
        struct ev_loop* loop = ev_default_loop(0);
        ev_timer_stop(loop, &e->gc_timer);
    }
    free(e);
    return 0;
}
