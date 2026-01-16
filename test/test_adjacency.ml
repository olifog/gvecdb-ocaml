(** Graph traversal and adjacency query tests *)

open Alcotest
open Test_common

(** {1 Basic adjacency tests} *)

let test_outbound_edges () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let c = ok_exn (Gvecdb.create_node db "person") in
  let _ = ok_exn (Gvecdb.create_edge db "knows" a b) in
  let _ = ok_exn (Gvecdb.create_edge db "knows" a c) in

  let outbound = ok_exn (Gvecdb.get_outbound_edges db a) in
  check int "a has 2 outbound" 2 (List.length outbound);

  let dsts = List.map (fun e -> e.Gvecdb.dst) outbound in
  check bool "b in destinations" true (List.mem b dsts);
  check bool "c in destinations" true (List.mem c dsts)

let test_inbound_edges () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let c = ok_exn (Gvecdb.create_node db "person") in
  let _ = ok_exn (Gvecdb.create_edge db "knows" b a) in
  let _ = ok_exn (Gvecdb.create_edge db "knows" c a) in

  let inbound = ok_exn (Gvecdb.get_inbound_edges db a) in
  check int "a has 2 inbound" 2 (List.length inbound);

  let srcs = List.map (fun e -> e.Gvecdb.src) inbound in
  check bool "b in sources" true (List.mem b srcs);
  check bool "c in sources" true (List.mem c srcs)

let test_no_outbound_edges () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let outbound = ok_exn (Gvecdb.get_outbound_edges db a) in
  check int "isolated node has no outbound" 0 (List.length outbound)

let test_no_inbound_edges () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let inbound = ok_exn (Gvecdb.get_inbound_edges db a) in
  check int "isolated node has no inbound" 0 (List.length inbound)

(** {1 Type-filtered adjacency tests} *)

let test_outbound_by_type () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let c = ok_exn (Gvecdb.create_node db "person") in
  let _ = ok_exn (Gvecdb.create_edge db "knows" a b) in
  let _ = ok_exn (Gvecdb.create_edge db "likes" a c) in
  let _ = ok_exn (Gvecdb.create_edge db "follows" a b) in

  let knows = ok_exn (Gvecdb.get_outbound_edges_by_type db a "knows") in
  let likes = ok_exn (Gvecdb.get_outbound_edges_by_type db a "likes") in
  let follows = ok_exn (Gvecdb.get_outbound_edges_by_type db a "follows") in

  check int "1 knows edge" 1 (List.length knows);
  check int "1 likes edge" 1 (List.length likes);
  check int "1 follows edge" 1 (List.length follows)

let test_inbound_by_type () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let c = ok_exn (Gvecdb.create_node db "person") in
  let _ = ok_exn (Gvecdb.create_edge db "knows" b a) in
  let _ = ok_exn (Gvecdb.create_edge db "likes" c a) in
  let _ = ok_exn (Gvecdb.create_edge db "knows" c a) in

  let knows = ok_exn (Gvecdb.get_inbound_edges_by_type db a "knows") in
  let likes = ok_exn (Gvecdb.get_inbound_edges_by_type db a "likes") in

  check int "2 knows edges" 2 (List.length knows);
  check int "1 likes edge" 1 (List.length likes)

let test_nonexistent_edge_type () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let _ = ok_exn (Gvecdb.create_edge db "knows" a b) in

  let phantom = ok_exn (Gvecdb.get_outbound_edges_by_type db a "nonexistent") in
  check int "no edges of nonexistent type" 0 (List.length phantom)

(** {1 Edge info correctness} *)

let test_edge_info_in_query_results () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let edge = ok_exn (Gvecdb.create_edge db "knows" a b) in

  let outbound = ok_exn (Gvecdb.get_outbound_edges db a) in
  match outbound with
  | [ info ] ->
      check int64 "correct edge id" edge info.id;
      check string "correct edge type" "knows" info.edge_type;
      check int64 "correct src" a info.src;
      check int64 "correct dst" b info.dst
  | _ -> fail "expected exactly one edge"

(** {1 Self-loop tests} *)

let test_self_loop_adjacency () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let _ = ok_exn (Gvecdb.create_edge db "knows" a a) in

  let outbound = ok_exn (Gvecdb.get_outbound_edges db a) in
  let inbound = ok_exn (Gvecdb.get_inbound_edges db a) in

  check int "self-loop in outbound" 1 (List.length outbound);
  check int "self-loop in inbound" 1 (List.length inbound)

let test_multiple_self_loops () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let _ = ok_exn (Gvecdb.create_edge db "knows" a a) in
  let _ = ok_exn (Gvecdb.create_edge db "likes" a a) in

  let all = ok_exn (Gvecdb.get_outbound_edges db a) in
  let knows = ok_exn (Gvecdb.get_outbound_edges_by_type db a "knows") in
  let likes = ok_exn (Gvecdb.get_outbound_edges_by_type db a "likes") in

  check int "2 total self-loops" 2 (List.length all);
  check int "1 knows self-loop" 1 (List.length knows);
  check int "1 likes self-loop" 1 (List.length likes)

(** {1 Multi-edge tests (parallel edges)} *)

let test_parallel_edges_same_type () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let e1 = ok_exn (Gvecdb.create_edge db "knows" a b) in
  let e2 = ok_exn (Gvecdb.create_edge db "knows" a b) in
  let e3 = ok_exn (Gvecdb.create_edge db "knows" a b) in

  let outbound = ok_exn (Gvecdb.get_outbound_edges_by_type db a "knows") in
  check int "3 parallel edges" 3 (List.length outbound);

  let edge_ids = List.map (fun e -> e.Gvecdb.id) outbound in
  check bool "e1 present" true (List.mem e1 edge_ids);
  check bool "e2 present" true (List.mem e2 edge_ids);
  check bool "e3 present" true (List.mem e3 edge_ids)

let test_bidirectional_edges () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let _ = ok_exn (Gvecdb.create_edge db "knows" a b) in
  let _ = ok_exn (Gvecdb.create_edge db "knows" b a) in

  let a_out = ok_exn (Gvecdb.get_outbound_edges db a) in
  let a_in = ok_exn (Gvecdb.get_inbound_edges db a) in
  let b_out = ok_exn (Gvecdb.get_outbound_edges db b) in
  let b_in = ok_exn (Gvecdb.get_inbound_edges db b) in

  check int "a outbound" 1 (List.length a_out);
  check int "a inbound" 1 (List.length a_in);
  check int "b outbound" 1 (List.length b_out);
  check int "b inbound" 1 (List.length b_in)

(** {1 Deletion effects on adjacency} *)

let test_delete_edge_updates_adjacency () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let e1 = ok_exn (Gvecdb.create_edge db "knows" a b) in
  let _e2 = ok_exn (Gvecdb.create_edge db "likes" a b) in

  check int "2 outbound before" 2
    (List.length (ok_exn (Gvecdb.get_outbound_edges db a)));

  ok_exn (Gvecdb.delete_edge db e1);

  let remaining = ok_exn (Gvecdb.get_outbound_edges db a) in
  check int "1 outbound after" 1 (List.length remaining);
  check string "likes remains" "likes" (List.hd remaining).edge_type

let test_delete_middle_edge () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let e1 = ok_exn (Gvecdb.create_edge db "knows" a b) in
  let e2 = ok_exn (Gvecdb.create_edge db "knows" a b) in
  let e3 = ok_exn (Gvecdb.create_edge db "knows" a b) in

  ok_exn (Gvecdb.delete_edge db e2);

  let remaining = ok_exn (Gvecdb.get_outbound_edges db a) in
  check int "2 edges remain" 2 (List.length remaining);
  let ids = List.map (fun e -> e.Gvecdb.id) remaining in
  check bool "e1 remains" true (List.mem e1 ids);
  check bool "e2 gone" false (List.mem e2 ids);
  check bool "e3 remains" true (List.mem e3 ids)

(** {1 Large graph tests} *)

let test_many_outbound_edges () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let targets =
    List.init 100 (fun _ -> ok_exn (Gvecdb.create_node db "person"))
  in
  List.iter
    (fun t -> ignore (ok_exn (Gvecdb.create_edge db "knows" a t)))
    targets;

  let outbound = ok_exn (Gvecdb.get_outbound_edges db a) in
  check int "100 outbound edges" 100 (List.length outbound)

let test_many_inbound_edges () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let sources =
    List.init 100 (fun _ -> ok_exn (Gvecdb.create_node db "person"))
  in
  List.iter
    (fun s -> ignore (ok_exn (Gvecdb.create_edge db "knows" s a)))
    sources;

  let inbound = ok_exn (Gvecdb.get_inbound_edges db a) in
  check int "100 inbound edges" 100 (List.length inbound)

let test_hub_node () =
  with_temp_db "adj" @@ fun db ->
  let hub = ok_exn (Gvecdb.create_node db "person") in
  let nodes = List.init 50 (fun _ -> ok_exn (Gvecdb.create_node db "person")) in
  List.iter
    (fun n ->
      ignore (ok_exn (Gvecdb.create_edge db "knows" hub n));
      ignore (ok_exn (Gvecdb.create_edge db "knows" n hub)))
    nodes;

  let out = ok_exn (Gvecdb.get_outbound_edges db hub) in
  let inb = ok_exn (Gvecdb.get_inbound_edges db hub) in

  check int "50 outbound from hub" 50 (List.length out);
  check int "50 inbound to hub" 50 (List.length inb)

(** {1 Transaction tests} *)

let test_adjacency_in_transaction () =
  with_temp_db "adj" @@ fun db ->
  let result =
    Gvecdb.with_transaction db (fun txn ->
        let a = ok_exn (Gvecdb.create_node db ~txn "person") in
        let b = ok_exn (Gvecdb.create_node db ~txn "person") in
        let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" a b) in
        ok_exn (Gvecdb.get_outbound_edges db ~txn a))
  in
  match result with
  | Some edges -> check int "edge visible in txn" 1 (List.length edges)
  | None -> fail "transaction failed"

(** {1 Test runner} *)

let basic_tests =
  [
    ("outbound_edges", `Quick, test_outbound_edges);
    ("inbound_edges", `Quick, test_inbound_edges);
    ("no_outbound", `Quick, test_no_outbound_edges);
    ("no_inbound", `Quick, test_no_inbound_edges);
  ]

let type_filter_tests =
  [
    ("outbound_by_type", `Quick, test_outbound_by_type);
    ("inbound_by_type", `Quick, test_inbound_by_type);
    ("nonexistent_type", `Quick, test_nonexistent_edge_type);
  ]

let info_tests =
  [ ("edge_info_correct", `Quick, test_edge_info_in_query_results) ]

let self_loop_tests =
  [
    ("self_loop", `Quick, test_self_loop_adjacency);
    ("multiple_self_loops", `Quick, test_multiple_self_loops);
  ]

let multi_edge_tests =
  [
    ("parallel_edges", `Quick, test_parallel_edges_same_type);
    ("bidirectional", `Quick, test_bidirectional_edges);
  ]

let deletion_tests =
  [
    ("delete_updates_adj", `Quick, test_delete_edge_updates_adjacency);
    ("delete_middle", `Quick, test_delete_middle_edge);
  ]

let large_tests =
  [
    ("many_outbound", `Quick, test_many_outbound_edges);
    ("many_inbound", `Quick, test_many_inbound_edges);
    ("hub_node", `Quick, test_hub_node);
  ]

let txn_tests = [ ("adjacency_in_txn", `Quick, test_adjacency_in_transaction) ]

let () =
  run "Adjacency"
    [
      ("basic", basic_tests);
      ("type_filter", type_filter_tests);
      ("edge_info", info_tests);
      ("self_loops", self_loop_tests);
      ("multi_edge", multi_edge_tests);
      ("deletion", deletion_tests);
      ("large_graph", large_tests);
      ("transactions", txn_tests);
    ]
