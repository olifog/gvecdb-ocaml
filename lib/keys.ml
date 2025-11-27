(** key encoding/decoding helpers for LMDB indexes *)

open Types

module Bigstring = Bigstringaf

let encode_id_bs (id : int64) : bigstring =
  let buf = Bigstring.create 8 in
  Bigstring.set_int64_be buf 0 id;
  buf

let decode_id_bs (bs : bigstring) : int64 =
  Bigstring.get_int64_be bs 0

(** Adjacency key: (node_id, intern_id, opposite_id, edge_id) -> 32 bytes *)
let encode_adjacency_bs ~node_id ~intern_id ~opposite_id ~edge_id : bigstring =
  let buf = Bigstring.create 32 in
  Bigstring.set_int64_be buf 0 node_id;
  Bigstring.set_int64_be buf 8 intern_id;
  Bigstring.set_int64_be buf 16 opposite_id;
  Bigstring.set_int64_be buf 24 edge_id;
  buf

let decode_adjacency_bs (bs : bigstring) : node_id * intern_id * node_id * edge_id =
  let node_id = Bigstring.get_int64_be bs 0 in
  let intern_id = Bigstring.get_int64_be bs 8 in
  let opposite_id = Bigstring.get_int64_be bs 16 in
  let edge_id = Bigstring.get_int64_be bs 24 in
  (node_id, intern_id, opposite_id, edge_id)

let encode_adjacency_prefix_bs ?node_id ?intern_id ?opposite_id () : bigstring =
  match node_id, intern_id, opposite_id with
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
