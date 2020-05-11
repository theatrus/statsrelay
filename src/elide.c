#include <stdlib.h>
#include <string.h>

#include "elide.h"

int elide_init(elide_t** e, int skip) {
    elide_t* el = malloc(sizeof(elide_t));
    int res = hashmap_init(0, &el->elide_map);
    el->skip = skip;
    *e = el;
    return res;
}

int elide_mark(elide_t* e, char* key, struct timeval now) {
    elide_value_t *v;
    int res = hashmap_get(e->elide_map, key, (void**)&v);
    if (res == -1) {
        v = calloc(sizeof(elide_value_t), 1);
        v->generations = e->skip;
        hashmap_put(e->elide_map, key, v);
    }
    memcpy(&v->last_seen, &now, sizeof(struct timeval));
    return v->generations++;
}

int elide_unmark(elide_t* e, char *key, struct timeval now) {
    elide_value_t* v;
    int res = hashmap_get(e->elide_map, key, (void**)&v);
    if (res == -1) {
        v = calloc(sizeof(elide_value_t), 1);
        hashmap_put(e->elide_map, key, v);
    }
    memcpy(&v->last_seen, &now, sizeof(struct timeval));
    v->generations = e->skip;
    return e->skip;
}

static int elide_delete_cb(void* data, const char *key, void* value) {
    free(value);
    return 0;
}

int elide_destroy(elide_t* e) {
    hashmap_iter(e->elide_map, elide_delete_cb, NULL);
    hashmap_destroy(e->elide_map);
    free(e);
    return 0;
}

struct cb_info {
    struct timeval cutoff;
};

static int elide_gc_cb(void* data, const char *key, void* value) {
    struct cb_info* info = (struct cb_info*)data;
    elide_value_t* oldvalue = (elide_value_t*)value;

    /* Values not yet expired are not marked for removal */
    if (oldvalue->last_seen.tv_sec > info->cutoff.tv_sec) {
        return 0;
    }
    return 1;
}

int elide_gc(elide_t* e, struct timeval cutoff) {
    struct cb_info cb = {
            .cutoff = cutoff
    };
    int pre_count = hashmap_size(e->elide_map);
    hashmap_filter(e->elide_map, elide_gc_cb, (void*)&cb);
    return pre_count - hashmap_size(e->elide_map);
}
