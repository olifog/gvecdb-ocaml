open Alcotest
open Test_common

let test_delete_node_cascades () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = ok_exn (Gvecdb.create_node db "person") in
  let bob = ok_exn (Gvecdb.create_node db "person") in
  let charlie = ok_exn (Gvecdb.create_node db "person") in
  let e1 = ok_exn (Gvecdb.create_edge db "knows" alice bob) in
  let e2 = ok_exn (Gvecdb.create_edge db "knows" charlie alice) in
  let info = ok_exn (Gvecdb.get_node_info db alice) in
  check string "correct type" "person" info.node_type;
  ok_exn (Gvecdb.delete_node db alice);
  check bool "alice gone" false (ok_exn (Gvecdb.node_exists db alice));
  check bool "e1 gone" false (ok_exn (Gvecdb.edge_exists db e1));
  check bool "e2 gone" false (ok_exn (Gvecdb.edge_exists db e2));
  (match Gvecdb.get_node_info db alice with
  | Error (Gvecdb.Node_not_found _) -> ()
  | _ -> fail "expected Node_not_found");
  match Gvecdb.delete_node db 999999L with
  | Error (Gvecdb.Node_not_found _) -> ()
  | _ -> fail "expected Node_not_found"

let test_delete_edge_cleans_adjacency () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = ok_exn (Gvecdb.create_node db "person") in
  let bob = ok_exn (Gvecdb.create_node db "person") in
  let edge = ok_exn (Gvecdb.create_edge db "knows" alice bob) in
  let info = ok_exn (Gvecdb.get_edge_info db edge) in
  check string "correct type" "knows" info.edge_type;
  check int64 "correct src" alice info.src;
  check int64 "correct dst" bob info.dst;
  ok_exn (Gvecdb.delete_edge db edge);
  check bool "edge gone" false (ok_exn (Gvecdb.edge_exists db edge));
  check int "outbound cleared" 0
    (List.length (ok_exn (Gvecdb.get_outbound_edges db alice ())));
  check int "inbound cleared" 0
    (List.length (ok_exn (Gvecdb.get_inbound_edges db bob ())))

let test_props_roundtrip () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = create_person db "Alice" 30 "alice@example.com" "Engineer" in
  check string "node name" "Alice" (get_person_name db alice);
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let edge = create_knows_edge db a b 1234567890L "Met at work" 0.75 in
  let since, context, strength =
    read_edge_props_capnp db edge SchemaReader.Reader.Knows.of_message (fun r ->
        ( SchemaReader.Reader.Knows.since_get r,
          SchemaReader.Reader.Knows.context_get r,
          SchemaReader.Reader.Knows.strength_get r ))
  in
  check int64 "since" 1234567890L since;
  check string "context" "Met at work" context;
  check (float 0.01) "strength" 0.75 strength

let test_props_update () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = create_person db "Alice" 30 "alice@example.com" "Engineer" in
  let builder = SchemaBuilder.Builder.Person.init_root () in
  SchemaBuilder.Builder.Person.name_set builder "Alice Smith";
  SchemaBuilder.Builder.Person.age_set_int_exn builder 31;
  let bs = capnp_to_bigstring SchemaBuilder.Builder.Person.to_message builder in
  ok_exn (Gvecdb.set_node_props db alice "person" bs);
  check string "name updated" "Alice Smith" (get_person_name db alice);
  let edge_a = ok_exn (Gvecdb.create_node db "person") in
  let edge_b = ok_exn (Gvecdb.create_node db "person") in
  let edge = ok_exn (Gvecdb.create_edge db "knows" edge_a edge_b) in
  let kb = SchemaBuilder.Builder.Knows.init_root () in
  SchemaBuilder.Builder.Knows.context_set kb "test";
  let kbs = capnp_to_bigstring SchemaBuilder.Builder.Knows.to_message kb in
  ok_exn (Gvecdb.set_edge_props db edge kbs);
  let info = ok_exn (Gvecdb.get_edge_info db edge) in
  check int64 "edge meta src preserved" edge_a info.src;
  check string "edge meta type preserved" "knows" info.edge_type

let test_persistence_across_reopen () =
  let path = temp_db_path "persist" in
  (try Sys.remove path with _ -> ());
  let alice_id, bob_id, edge_id =
    let db = ok_exn (Gvecdb.create path) in
    register_schemas db;
    let alice = create_person db "Alice" 30 "alice@test.com" "Engineer" in
    let bob = create_person db "Bob" 25 "bob@test.com" "Designer" in
    let edge = create_knows_edge db alice bob 1234567890L "Work" 0.8 in
    Gvecdb.close db;
    (alice, bob, edge)
  in
  Fun.protect
    ~finally:(fun () -> cleanup_db_files path)
    (fun () ->
      let db = ok_exn (Gvecdb.create path) in
      Fun.protect
        ~finally:(fun () -> Gvecdb.close db)
        (fun () ->
          check bool "alice exists" true
            (ok_exn (Gvecdb.node_exists db alice_id));
          check string "alice name" "Alice" (get_person_name db alice_id);
          check string "bob name" "Bob" (get_person_name db bob_id);
          let edge_info = ok_exn (Gvecdb.get_edge_info db edge_id) in
          check int64 "edge src" alice_id edge_info.src;
          check string "edge type" "knows" edge_info.edge_type;
          check int "outbound count" 1
            (List.length (ok_exn (Gvecdb.get_outbound_edges db alice_id ())))))

let () =
  run "Basic"
    [
      ( "crud",
        [
          ("delete_node_cascades", `Quick, test_delete_node_cascades);
          ( "delete_edge_cleans_adjacency",
            `Quick,
            test_delete_edge_cleans_adjacency );
        ] );
      ( "properties",
        [
          ("props_roundtrip", `Quick, test_props_roundtrip);
          ("props_update", `Quick, test_props_update);
        ] );
      ( "persistence",
        [
          ("persistence_across_reopen", `Quick, test_persistence_across_reopen);
        ] );
    ]
