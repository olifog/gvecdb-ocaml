(** Tests for key encoding/decoding in the Keys module.
    
    Verifies round-trip encoding and proper byte ordering for LMDB indexes. *)

open Alcotest

(* Access internal modules through Gvecdb *)
module Keys = Gvecdb__Keys
module Bigstring = Bigstringaf

(** {1 ID encoding tests} *)

let test_id_roundtrip () =
  let test_values = [0L; 1L; 255L; 256L; 65535L; 65536L; 
                     Int64.max_int; Int64.min_int; -1L] in
  List.iter (fun id ->
    let encoded = Keys.encode_id_bs id in
    let decoded = Keys.decode_id_bs encoded in
    check int64 (Printf.sprintf "roundtrip %Ld" id) id decoded
  ) test_values

let test_id_encoding_size () =
  let encoded = Keys.encode_id_bs 12345L in
  check int "id encoding is 8 bytes" 8 (Bigstring.length encoded)

let bigstring_compare a b =
  let len_a = Bigstring.length a in
  let len_b = Bigstring.length b in
  let min_len = min len_a len_b in
  let rec loop i =
    if i >= min_len then compare len_a len_b
    else
      let ca = Char.code (Bigstring.get a i) in
      let cb = Char.code (Bigstring.get b i) in
      if ca <> cb then compare ca cb
      else loop (i + 1)
  in
  loop 0

let test_id_big_endian_ordering () =
  (* Big-endian means larger IDs sort after smaller IDs in LMDB *)
  let small = Keys.encode_id_bs 100L in
  let large = Keys.encode_id_bs 200L in
  (* Compare byte-by-byte *)
  let cmp = bigstring_compare small large in
  check bool "small < large in byte order" true (cmp < 0)

(** {1 Adjacency key tests} *)

let test_adjacency_roundtrip () =
  let node_id = 123L in
  let intern_id = 456L in
  let opposite_id = 789L in
  let edge_id = 101112L in
  let encoded = Keys.encode_adjacency_bs ~node_id ~intern_id ~opposite_id ~edge_id in
  let (n, i, o, e) = Keys.decode_adjacency_bs encoded in
  check int64 "node_id" node_id n;
  check int64 "intern_id" intern_id i;
  check int64 "opposite_id" opposite_id o;
  check int64 "edge_id" edge_id e

let test_adjacency_encoding_size () =
  let encoded = Keys.encode_adjacency_bs 
    ~node_id:1L ~intern_id:2L ~opposite_id:3L ~edge_id:4L in
  check int "adjacency key is 32 bytes" 32 (Bigstring.length encoded)

let test_adjacency_prefix_node_only () =
  let prefix = Keys.encode_adjacency_prefix_bs ~node_id:123L () in
  check int "node-only prefix is 8 bytes" 8 (Bigstring.length prefix)

let test_adjacency_prefix_node_and_type () =
  let prefix = Keys.encode_adjacency_prefix_bs ~node_id:123L ~intern_id:456L () in
  check int "node+type prefix is 16 bytes" 16 (Bigstring.length prefix)

let test_adjacency_prefix_full () =
  let prefix = Keys.encode_adjacency_prefix_bs 
    ~node_id:123L ~intern_id:456L ~opposite_id:789L () in
  check int "full prefix (no edge) is 24 bytes" 24 (Bigstring.length prefix)

let test_adjacency_prefix_empty () =
  let prefix = Keys.encode_adjacency_prefix_bs () in
  check int "empty prefix is 0 bytes" 0 (Bigstring.length prefix)

let test_adjacency_key_ordering () =
  (* Keys should sort by (node_id, intern_id, opposite_id, edge_id) *)
  let k1 = Keys.encode_adjacency_bs ~node_id:1L ~intern_id:1L ~opposite_id:1L ~edge_id:1L in
  let k2 = Keys.encode_adjacency_bs ~node_id:1L ~intern_id:1L ~opposite_id:1L ~edge_id:2L in
  let k3 = Keys.encode_adjacency_bs ~node_id:1L ~intern_id:1L ~opposite_id:2L ~edge_id:1L in
  let k4 = Keys.encode_adjacency_bs ~node_id:1L ~intern_id:2L ~opposite_id:1L ~edge_id:1L in
  let k5 = Keys.encode_adjacency_bs ~node_id:2L ~intern_id:1L ~opposite_id:1L ~edge_id:1L in
  
  check bool "k1 < k2 (edge_id)" true (bigstring_compare k1 k2 < 0);
  check bool "k2 < k3 (opposite_id)" true (bigstring_compare k2 k3 < 0);
  check bool "k3 < k4 (intern_id)" true (bigstring_compare k3 k4 < 0);
  check bool "k4 < k5 (node_id)" true (bigstring_compare k4 k5 < 0)

(** {1 Edge metadata tests} *)

let test_edge_meta_roundtrip () =
  let type_id = 111L in
  let src = 222L in
  let dst = 333L in
  let encoded = Keys.encode_edge_meta ~type_id ~src ~dst in
  let (t, s, d) = Keys.decode_edge_meta encoded in
  check int64 "type_id" type_id t;
  check int64 "src" src s;
  check int64 "dst" dst d

let test_edge_meta_size () =
  let encoded = Keys.encode_edge_meta ~type_id:1L ~src:2L ~dst:3L in
  check int "edge meta is 24 bytes" 24 (Bigstring.length encoded)

(** {1 Prefix matching tests} *)

let test_has_prefix_true () =
  let key = Keys.encode_adjacency_bs ~node_id:123L ~intern_id:456L ~opposite_id:789L ~edge_id:101L in
  let prefix = Keys.encode_adjacency_prefix_bs ~node_id:123L ~intern_id:456L () in
  check bool "key has prefix" true (Keys.bigstring_has_prefix ~prefix key)

let test_has_prefix_false () =
  let key = Keys.encode_adjacency_bs ~node_id:123L ~intern_id:456L ~opposite_id:789L ~edge_id:101L in
  let wrong_prefix = Keys.encode_adjacency_prefix_bs ~node_id:999L () in
  check bool "key doesn't have wrong prefix" false (Keys.bigstring_has_prefix ~prefix:wrong_prefix key)

let test_has_prefix_equal_length () =
  let key = Keys.encode_id_bs 123L in
  let prefix = Keys.encode_id_bs 123L in
  check bool "equal length prefix match" true (Keys.bigstring_has_prefix ~prefix key)

let test_has_prefix_longer_prefix () =
  let key = Keys.encode_id_bs 123L in
  let prefix = Keys.encode_adjacency_bs ~node_id:123L ~intern_id:1L ~opposite_id:1L ~edge_id:1L in
  check bool "longer prefix doesn't match" false (Keys.bigstring_has_prefix ~prefix key)

let test_has_prefix_empty () =
  let key = Keys.encode_id_bs 123L in
  let empty = Bigstring.create 0 in
  check bool "empty prefix matches everything" true (Keys.bigstring_has_prefix ~prefix:empty key)

(** {1 Edge cases} *)

let test_zero_values () =
  let encoded = Keys.encode_adjacency_bs ~node_id:0L ~intern_id:0L ~opposite_id:0L ~edge_id:0L in
  let (n, i, o, e) = Keys.decode_adjacency_bs encoded in
  check int64 "zero node_id" 0L n;
  check int64 "zero intern_id" 0L i;
  check int64 "zero opposite_id" 0L o;
  check int64 "zero edge_id" 0L e

let test_max_values () =
  let max = Int64.max_int in
  let encoded = Keys.encode_adjacency_bs ~node_id:max ~intern_id:max ~opposite_id:max ~edge_id:max in
  let (n, i, o, e) = Keys.decode_adjacency_bs encoded in
  check int64 "max node_id" max n;
  check int64 "max intern_id" max i;
  check int64 "max opposite_id" max o;
  check int64 "max edge_id" max e

(** {1 Test runner} *)

let id_tests = [
  "id_roundtrip", `Quick, test_id_roundtrip;
  "id_size", `Quick, test_id_encoding_size;
  "id_ordering", `Quick, test_id_big_endian_ordering;
]

let adjacency_tests = [
  "adjacency_roundtrip", `Quick, test_adjacency_roundtrip;
  "adjacency_size", `Quick, test_adjacency_encoding_size;
  "prefix_node_only", `Quick, test_adjacency_prefix_node_only;
  "prefix_node_type", `Quick, test_adjacency_prefix_node_and_type;
  "prefix_full", `Quick, test_adjacency_prefix_full;
  "prefix_empty", `Quick, test_adjacency_prefix_empty;
  "key_ordering", `Quick, test_adjacency_key_ordering;
]

let edge_meta_tests = [
  "edge_meta_roundtrip", `Quick, test_edge_meta_roundtrip;
  "edge_meta_size", `Quick, test_edge_meta_size;
]

let prefix_tests = [
  "has_prefix_true", `Quick, test_has_prefix_true;
  "has_prefix_false", `Quick, test_has_prefix_false;
  "has_prefix_equal", `Quick, test_has_prefix_equal_length;
  "has_prefix_longer", `Quick, test_has_prefix_longer_prefix;
  "has_prefix_empty", `Quick, test_has_prefix_empty;
]

let edge_case_tests = [
  "zero_values", `Quick, test_zero_values;
  "max_values", `Quick, test_max_values;
]

let () =
  run "Keys" [
    "id_encoding", id_tests;
    "adjacency", adjacency_tests;
    "edge_meta", edge_meta_tests;
    "prefix_matching", prefix_tests;
    "edge_cases", edge_case_tests;
  ]

