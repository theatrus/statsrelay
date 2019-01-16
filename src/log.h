#ifndef STATSRELAY_LOG_H
#define STATSRELAY_LOG_H

#include <stdarg.h>
#include <stdbool.h>

#define noinline __attribute__((__noinline__))
#define stats_printf __attribute__((__format__(__printf__, 1, 2)))
#define unlikely(x) __builtin_expect(!!(x), 0)

enum statsrelay_log_level {
    STATSRELAY_LOG_DEBUG   = 10,
    STATSRELAY_LOG_INFO    = 20,
    STATSRELAY_LOG_WARN    = 30,
    STATSRELAY_LOG_ERROR   = 40
};

// TODO (CEV): most of these should be marked 'noinline'

// set verbose logging, i.e. send logs to stderr
void stats_log_verbose(bool verbose);

// set syslog logging, i.e. send logs to syslog (defaults to true)
void stats_log_syslog(bool syslog);

void stats_set_log_level(enum statsrelay_log_level level);

enum statsrelay_log_level stats_get_log_level() __attribute__((pure));

// log a message
void noinline stats_log_impl(const char *format, ...) stats_printf;

// log a debug message
void noinline stats_debug_log_impl(const char *format, ...) stats_printf;

// log an error message
void noinline stats_error_log_impl(const char *format, ...) stats_printf;

// finish logging; this ensures that the internally allocated buffer is freed;
// it can safely be called multiple times
void stats_log_end(void);

#define stats_log(format, ...) \
	if (unlikely(stats_get_log_level() <= STATSRELAY_LOG_INFO)) \
		stats_log_impl(format, ##__VA_ARGS__);

#define stats_error_log(format, ...) \
	if (unlikely(stats_get_log_level() <= STATSRELAY_LOG_ERROR)) \
		stats_error_log_impl(format, ##__VA_ARGS__);

#define stats_debug_log(format, ...) \
	if (unlikely(stats_get_log_level() <= STATSRELAY_LOG_DEBUG)) \
		stats_debug_log_impl(format, ##__VA_ARGS__);

#endif
