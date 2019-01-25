#undef NDEBUG

#include <assert.h>
#include <string.h>

#include "../log.h"
#include "../validate.h"

void test_validate_stat() {
    validate_parsed_result_t result;

    static const char* exp1 = "a.b.c.__tag1=v1.__tag2=v2:v2:42.000|ms";

    assert(0 == validate_statsd(exp1, strlen(exp1), &result));
    assert(42.0 == result.value);
    assert(METRIC_TIMER == result.type);
}

void test_parse_presampling_value() {
    validate_parsed_result_t result;

    static const char* exp1 = "test.srv.req:2.5|ms|@0.2";

    assert(0 == validate_statsd(exp1, strlen(exp1), &result));
    assert(2.5 == result.value);
    assert(0.2 == result.presampling_value);
    assert(METRIC_TIMER == result.type);
}

void test_line_not_modified() {
    const char *lines[] = {
        "a.b.c.__tag1=v1.__tag2=v2:v2:42.000|ms",
        "test.srv.req:2.5|ms|@0.2",
    };
    const int lines_len = sizeof(lines) / sizeof(char *);

    for (int i = 0; i < lines_len; i++) {
        const char *line = lines[i];
        const char *copy = strdup(line);

        validate_parsed_result_t result;
        assert(validate_statsd(line, strlen(line), &result) == 0);
        assert(strcmp(line, copy) == 0);
    }
}

int main() {
    stats_log_verbose(1);

    test_validate_stat();

    test_parse_presampling_value();

    test_line_not_modified();

    return 0;
}
