#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/fail.h>
#include <caml/bigarray.h>
#include <sys/mman.h>
#include <errno.h>
#include <string.h>

CAMLprim value gvecdb_msync(value ba) {
  CAMLparam1(ba);
  void *data = Caml_ba_data_val(ba);
  size_t len = Caml_ba_array_val(ba)->dim[0];
  if (len > 0 && msync(data, len, MS_SYNC) == -1) {
    char buf[256];
    snprintf(buf, sizeof(buf), "msync failed: %s", strerror(errno));
    caml_failwith(buf);
  }
  CAMLreturn(Val_unit);
}
