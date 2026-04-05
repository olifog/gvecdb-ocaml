#include <time.h>
#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/fail.h>

CAMLprim value bench_clock_monotonic_ns(value unit)
{
    (void)unit;
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        caml_failwith("clock_gettime(CLOCK_MONOTONIC) failed");
    return caml_copy_double((double)ts.tv_sec * 1e9 + (double)ts.tv_nsec);
}
