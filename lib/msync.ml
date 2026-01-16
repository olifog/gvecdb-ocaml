(** msync bindings for flushing mmap'd memory to disk *)

external msync : Common.bigstring -> unit = "gvecdb_msync"
