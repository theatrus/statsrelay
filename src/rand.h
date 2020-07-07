#ifndef STATSRELAY_RAND_H
#define STATSRELAY_RAND_H

/**
 * Gather len bytes of entropy from the system entropy source,
 * e.g. /dev/urandom
 */
extern size_t rand_gather(char* bytes, size_t len);

#endif //STATSRELAY_RAND_H