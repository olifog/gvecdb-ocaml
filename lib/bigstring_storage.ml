(** MessageStorage implementation for bigstrings.
    implements Capnp.MessageStorage.S for use with Capnp.Message.Make. *)

module Bigstring = Bigstringaf
module Uint32 = Stdint.Uint32
module Uint64 = Stdint.Uint64

type t = Bigstring.t

let alloc size = 
  let buf = Bigstring.create size in
  for i = 0 to size - 1 do
    Bigstring.set buf i '\x00'
  done;
  buf

let release _ = ()

let length = Bigstring.length

(* read operations*)
let get_uint8 s i = Char.code (Bigstring.get s i)
let get_uint16 s i = Bigstring.get_int16_le s i land 0xFFFF
let get_uint32 s i = Uint32.of_int32 (Bigstring.get_int32_le s i)
let get_uint64 s i = Uint64.of_int64 (Bigstring.get_int64_le s i)

let get_int8 s i = 
  let v = Char.code (Bigstring.get s i) in
  if v > 127 then v - 256 else v

let get_int16 s i = Bigstring.get_int16_sign_extended_le s i
let get_int32 s i = Bigstring.get_int32_le s i
let get_int64 s i = Bigstring.get_int64_le s i

(* write operations *)
let set_uint8 s i v =
  if v < 0 || v > 0xff then invalid_arg "Bigstring_storage.set_uint8";
  Bigstring.set s i (Char.chr v)

let set_uint16 s i v =
  if v < 0 || v > 0xffff then invalid_arg "Bigstring_storage.set_uint16";
  Bigstring.set_int16_le s i v

let set_uint32 s i v = Bigstring.set_int32_le s i (Uint32.to_int32 v)
let set_uint64 s i v = Bigstring.set_int64_le s i (Uint64.to_int64 v)

let set_int8 s i v =
  if v < -128 || v > 127 then invalid_arg "Bigstring_storage.set_int8";
  Bigstring.set s i (Char.chr (if v < 0 then v + 256 else v))

let set_int16 s i v =
  if v < -32768 || v > 32767 then invalid_arg "Bigstring_storage.set_int16";
  Bigstring.set_int16_le s i v

let set_int32 s i v = Bigstring.set_int32_le s i v
let set_int64 s i v = Bigstring.set_int64_le s i v

(* blit operations *)
let blit ~src ~src_pos ~dst ~dst_pos ~len =
  Bigstring.blit src ~src_off:src_pos dst ~dst_off:dst_pos ~len

let blit_to_bytes ~src ~src_pos ~dst ~dst_pos ~len =
  Bigstring.blit_to_bytes src ~src_off:src_pos dst ~dst_off:dst_pos ~len

let blit_from_string ~src ~src_pos ~dst ~dst_pos ~len =
  Bigstring.blit_from_string src ~src_off:src_pos dst ~dst_off:dst_pos ~len

let zero_out buf ~pos ~len =
  for i = pos to pos + len - 1 do
    Bigstring.set buf i '\x00'
  done
