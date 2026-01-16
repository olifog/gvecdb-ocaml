(** capnproto message backed by bigstrings. message segments can point directly
    into mmap'd memory without copying. *)

include Capnp.Message.Make (Bigstring_storage)
