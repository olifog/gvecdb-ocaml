(** key encoding/decoding helpers for LMDB indexes *)

open Types
module Bigstring = Bigstringaf

let encode_id_bs (id : int64) : bigstring =
  let buf = Bigstring.create 8 in
  Bigstring.set_int64_be buf 0 id;
  buf

let decode_id_bs (bs : bigstring) : int64 =
  if Bigstring.length bs < 8 then
    invalid_arg "Keys.decode_id_bs: buffer too short (need 8 bytes)";
  Bigstring.get_int64_be bs 0

(** Adjacency key: (node_id, intern_id, opposite_id, edge_id) -> 32 bytes *)
let encode_adjacency_bs ~node_id ~intern_id ~opposite_id ~edge_id : bigstring =
  let buf = Bigstring.create 32 in
  Bigstring.set_int64_be buf 0 node_id;
  Bigstring.set_int64_be buf 8 intern_id;
  Bigstring.set_int64_be buf 16 opposite_id;
  Bigstring.set_int64_be buf 24 edge_id;
  buf

let decode_adjacency_bs (bs : bigstring) :
    node_id * intern_id * node_id * edge_id =
  if Bigstring.length bs < 32 then
    invalid_arg "Keys.decode_adjacency_bs: buffer too short (need 32 bytes)";
  let node_id = Bigstring.get_int64_be bs 0 in
  let intern_id = Bigstring.get_int64_be bs 8 in
  let opposite_id = Bigstring.get_int64_be bs 16 in
  let edge_id = Bigstring.get_int64_be bs 24 in
  (node_id, intern_id, opposite_id, edge_id)

let encode_adjacency_prefix_bs ?node_id ?intern_id ?opposite_id () : bigstring =
  match (node_id, intern_id, opposite_id) with
  | None, _, _ -> Bigstring.create 0
  | Some nid, None, _ -> encode_id_bs nid
  | Some nid, Some tid, None ->
      let buf = Bigstring.create 16 in
      Bigstring.set_int64_be buf 0 nid;
      Bigstring.set_int64_be buf 8 tid;
      buf
  | Some nid, Some tid, Some oid ->
      let buf = Bigstring.create 24 in
      Bigstring.set_int64_be buf 0 nid;
      Bigstring.set_int64_be buf 8 tid;
      Bigstring.set_int64_be buf 16 oid;
      buf

(** edge metadata: (type_id, src, dst) -> 24 bytes *)
let encode_edge_meta ~type_id ~src ~dst : bigstring =
  let buf = Bigstring.create 24 in
  Bigstring.set_int64_be buf 0 type_id;
  Bigstring.set_int64_be buf 8 src;
  Bigstring.set_int64_be buf 16 dst;
  buf

let decode_edge_meta (bs : bigstring) : intern_id * node_id * node_id =
  if Bigstring.length bs < 24 then
    invalid_arg "Keys.decode_edge_meta: buffer too short (need 24 bytes)";
  let type_id = Bigstring.get_int64_be bs 0 in
  let src = Bigstring.get_int64_be bs 8 in
  let dst = Bigstring.get_int64_be bs 16 in
  (type_id, src, dst)

let bigstring_has_prefix ~prefix (bs : bigstring) : bool =
  let prefix_len = Bigstring.length prefix in
  let bs_len = Bigstring.length bs in
  if bs_len < prefix_len then false
  else
    let rec loop i =
      if i >= prefix_len then true
      else if Bigstring.get bs i <> Bigstring.get prefix i then false
      else loop (i + 1)
    in
    loop 0

(** vector index key: (packed_owner_id, vector_tag_id, vector_id) -> 24 bytes
    packed_owner_id has bit 63 = 0 for node, 1 for edge; bits 0-62 = actual id
*)
let encode_vector_index_bs ~owner_kind ~owner_id ~vector_tag_id ~vector_id :
    bigstring =
  let buf = Bigstring.create 24 in
  let packed_owner = Owner.encode owner_kind owner_id in
  Bigstring.set_int64_be buf 0 packed_owner;
  Bigstring.set_int64_be buf 8 vector_tag_id;
  Bigstring.set_int64_be buf 16 vector_id;
  buf

let decode_vector_index_bs (bs : bigstring) : owner_kind * id * intern_id * id =
  if Bigstring.length bs < 24 then
    invalid_arg "Keys.decode_vector_index_bs: buffer too short (need 24 bytes)";
  let packed_owner = Bigstring.get_int64_be bs 0 in
  let owner_kind, owner_id = Owner.decode packed_owner in
  let vector_tag_id = Bigstring.get_int64_be bs 8 in
  let vector_id = Bigstring.get_int64_be bs 16 in
  (owner_kind, owner_id, vector_tag_id, vector_id)

let encode_vector_index_prefix_bs ~owner_kind ~owner_id ?vector_tag_id () :
    bigstring =
  let packed_owner = Owner.encode owner_kind owner_id in
  match vector_tag_id with
  | None -> encode_id_bs packed_owner
  | Some tid ->
      let buf = Bigstring.create 16 in
      Bigstring.set_int64_be buf 0 packed_owner;
      Bigstring.set_int64_be buf 8 tid;
      buf

(** vector owner value: (packed_owner_id, vector_tag_id, file_offset) -> 24
    bytes packed_owner_id has bit 63 = 0 for node, 1 for edge; bits 0-62 =
    actual id file_offset is the byte offset in the vectors.bin mmap file *)
let encode_vector_owner_bs ~owner_kind ~owner_id ~vector_tag_id ~file_offset :
    bigstring =
  let buf = Bigstring.create 24 in
  let packed_owner = Owner.encode owner_kind owner_id in
  Bigstring.set_int64_be buf 0 packed_owner;
  Bigstring.set_int64_be buf 8 vector_tag_id;
  Bigstring.set_int64_be buf 16 file_offset;
  buf

let decode_vector_owner_bs (bs : bigstring) :
    owner_kind * id * intern_id * int64 =
  if Bigstring.length bs < 24 then
    invalid_arg "Keys.decode_vector_owner_bs: buffer too short (need 24 bytes)";
  let packed_owner = Bigstring.get_int64_be bs 0 in
  let owner_kind, owner_id = Owner.decode packed_owner in
  let vector_tag_id = Bigstring.get_int64_be bs 8 in
  let file_offset = Bigstring.get_int64_be bs 16 in
  (owner_kind, owner_id, vector_tag_id, file_offset)
