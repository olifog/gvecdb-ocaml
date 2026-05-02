open Alcotest
open Test_common

let test_type_filtered_queries () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let c = ok_exn (Gvecdb.create_node db "person") in
  let _ = ok_exn (Gvecdb.create_edge db "knows" a b) in
  let _ = ok_exn (Gvecdb.create_edge db "likes" a c) in
  let _ = ok_exn (Gvecdb.create_edge db "follows" a b) in
  let _ = ok_exn (Gvecdb.create_edge db "knows" b a) in
  let _ = ok_exn (Gvecdb.create_edge db "likes" c a) in
  check int "a total outbound" 3
    (List.length (ok_exn (Gvecdb.get_outbound_edges db a ())));
  check int "a knows outbound" 1
    (List.length
       (ok_exn (Gvecdb.get_outbound_edges db a ~edge_type:"knows" ())));
  check int "a inbound" 2
    (List.length (ok_exn (Gvecdb.get_inbound_edges db a ())));
  check int "a knows inbound" 1
    (List.length (ok_exn (Gvecdb.get_inbound_edges db a ~edge_type:"knows" ())));
  check int "nonexistent type" 0
    (List.length
       (ok_exn (Gvecdb.get_outbound_edges db a ~edge_type:"nonexistent" ())));
  check int "isolated node" 0
    (let d = ok_exn (Gvecdb.create_node db "person") in
     List.length (ok_exn (Gvecdb.get_outbound_edges db d ())))

let test_self_loop () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let _ = ok_exn (Gvecdb.create_edge db "knows" a a) in
  let _ = ok_exn (Gvecdb.create_edge db "likes" a a) in
  check int "self-loops in outbound" 2
    (List.length (ok_exn (Gvecdb.get_outbound_edges db a ())));
  check int "self-loops in inbound" 2
    (List.length (ok_exn (Gvecdb.get_inbound_edges db a ())))

let test_parallel_edges () =
  with_temp_db "adj" @@ fun db ->
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let e1 = ok_exn (Gvecdb.create_edge db "knows" a b) in
  let e2 = ok_exn (Gvecdb.create_edge db "knows" a b) in
  let e3 = ok_exn (Gvecdb.create_edge db "knows" a b) in
  check int "3 parallel edges" 3
    (List.length
       (ok_exn (Gvecdb.get_outbound_edges db a ~edge_type:"knows" ())));
  let _ = ok_exn (Gvecdb.create_edge db "knows" b a) in
  check int "bidirectional a out" 3
    (List.length (ok_exn (Gvecdb.get_outbound_edges db a ())));
  check int "bidirectional a in" 1
    (List.length (ok_exn (Gvecdb.get_inbound_edges db a ())));
  ok_exn (Gvecdb.delete_edge db e2);
  let remaining = ok_exn (Gvecdb.get_outbound_edges db a ()) in
  check int "2 after delete" 2 (List.length remaining);
  let ids = List.map (fun e -> e.Gvecdb.id) remaining in
  check bool "e1 remains" true (List.mem e1 ids);
  check bool "e2 gone" false (List.mem e2 ids);
  check bool "e3 remains" true (List.mem e3 ids)

let () =
  run "Adjacency"
    [
      ( "queries",
        [
          ("type_filtered_queries", `Quick, test_type_filtered_queries);
          ("self_loop", `Quick, test_self_loop);
          ("parallel_edges_and_delete", `Quick, test_parallel_edges);
        ] );
    ]
