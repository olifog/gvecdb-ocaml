(** Basic CRUD operation tests using Alcotest *)

open Alcotest
open Test_common

(** {1 Node tests} *)

let test_create_node () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let node = Gvecdb.create_node db "person" in
  check bool "node exists after creation" true (Gvecdb.node_exists db node)

let test_node_info () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let node = Gvecdb.create_node db "person" in
  match Gvecdb.get_node_info db node with
  | Some info ->
    check int64 "correct id" node info.id;
    check string "correct type" "person" info.node_type
  | None -> fail "node info not found"

let test_delete_node () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let node = Gvecdb.create_node db "person" in
  check bool "exists before delete" true (Gvecdb.node_exists db node);
  Gvecdb.delete_node db node;
  check bool "not exists after delete" false (Gvecdb.node_exists db node);
  check (option reject) "info is None" None (Gvecdb.get_node_info db node)

let test_node_not_found () =
  with_temp_db "basic" @@ fun db ->
  check bool "nonexistent node" false (Gvecdb.node_exists db 999999L);
  check (option reject) "info is None" None (Gvecdb.get_node_info db 999999L)

let test_delete_nonexistent_node () =
  with_temp_db "basic" @@ fun db ->
  (* Should not raise *)
  Gvecdb.delete_node db 999999L;
  check pass "no exception" () ()

let test_delete_node_cascades_edges () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = Gvecdb.create_node db "person" in
  let bob = Gvecdb.create_node db "person" in
  let charlie = Gvecdb.create_node db "person" in
  let e1 = Gvecdb.create_edge db "knows" alice bob in
  let e2 = Gvecdb.create_edge db "knows" charlie alice in
  check bool "e1 exists before" true (Gvecdb.edge_exists db e1);
  check bool "e2 exists before" true (Gvecdb.edge_exists db e2);
  check int "alice outbound before" 1 (List.length (Gvecdb.get_outbound_edges db alice));
  check int "alice inbound before" 1 (List.length (Gvecdb.get_inbound_edges db alice));
  Gvecdb.delete_node db alice;
  check bool "alice not exists after" false (Gvecdb.node_exists db alice);
  check bool "e1 not exists after" false (Gvecdb.edge_exists db e1);
  check bool "e2 not exists after" false (Gvecdb.edge_exists db e2);
  check int "bob inbound after" 0 (List.length (Gvecdb.get_inbound_edges db bob));
  check int "charlie outbound after" 0 (List.length (Gvecdb.get_outbound_edges db charlie))

(** {1 Edge tests} *)

let test_create_edge () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = Gvecdb.create_node db "person" in
  let bob = Gvecdb.create_node db "person" in
  let edge = Gvecdb.create_edge db "knows" alice bob in
  check bool "edge exists" true (Gvecdb.edge_exists db edge)

let test_edge_info () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = Gvecdb.create_node db "person" in
  let bob = Gvecdb.create_node db "person" in
  let edge = Gvecdb.create_edge db "knows" alice bob in
  match Gvecdb.get_edge_info db edge with
  | Some info ->
    check int64 "correct id" edge info.id;
    check string "correct type" "knows" info.edge_type;
    check int64 "correct src" alice info.src;
    check int64 "correct dst" bob info.dst
  | None -> fail "edge info not found"

let test_self_edge () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = Gvecdb.create_node db "person" in
  let edge = Gvecdb.create_edge db "knows" alice alice in
  match Gvecdb.get_edge_info db edge with
  | Some info ->
    check int64 "src equals dst" info.src info.dst
  | None -> fail "self-edge info not found"

let test_delete_edge () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = Gvecdb.create_node db "person" in
  let bob = Gvecdb.create_node db "person" in
  let edge = Gvecdb.create_edge db "knows" alice bob in
  check bool "exists before delete" true (Gvecdb.edge_exists db edge);
  Gvecdb.delete_edge db edge;
  check bool "not exists after delete" false (Gvecdb.edge_exists db edge)

let test_delete_edge_cleans_adjacency () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = Gvecdb.create_node db "person" in
  let bob = Gvecdb.create_node db "person" in
  let edge = Gvecdb.create_edge db "knows" alice bob in
  check int "outbound before" 1 (List.length (Gvecdb.get_outbound_edges db alice));
  check int "inbound before" 1 (List.length (Gvecdb.get_inbound_edges db bob));
  Gvecdb.delete_edge db edge;
  check int "outbound after" 0 (List.length (Gvecdb.get_outbound_edges db alice));
  check int "inbound after" 0 (List.length (Gvecdb.get_inbound_edges db bob))

(** {1 Property tests} *)

let test_node_props_roundtrip () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = create_person db "Alice" 30 "alice@example.com" "Engineer" in
  let name = get_person_name db alice in
  check string "name roundtrip" "Alice" name

let test_edge_props_roundtrip () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = Gvecdb.create_node db "person" in
  let bob = Gvecdb.create_node db "person" in
  let edge = create_knows_edge db alice bob 1234567890L "Met at work" 0.75 in
  let (since, context, strength) = Gvecdb.get_edge_props_capnp db edge
    SchemaReader.Reader.Knows.of_message
    (fun r -> 
      SchemaReader.Reader.Knows.since_get r,
      SchemaReader.Reader.Knows.context_get r,
      SchemaReader.Reader.Knows.strength_get r) in
  check int64 "since" 1234567890L since;
  check string "context" "Met at work" context;
  check (float 0.01) "strength" 0.75 strength

let test_update_node_props () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = create_person db "Alice" 30 "alice@example.com" "Engineer" in
  check string "name before" "Alice" (get_person_name db alice);
  (* Update properties *)
  Gvecdb.set_node_props_capnp db alice "person"
    (fun b ->
      SchemaBuilder.Builder.Person.name_set b "Alice Smith";
      SchemaBuilder.Builder.Person.age_set_int_exn b 31)
    SchemaBuilder.Builder.Person.init_root
    SchemaBuilder.Builder.Person.to_message;
  check string "name after" "Alice Smith" (get_person_name db alice)

let test_edge_meta_preserved_after_props_update () =
  with_temp_db "basic" @@ fun db ->
  register_schemas db;
  let alice = Gvecdb.create_node db "person" in
  let bob = Gvecdb.create_node db "person" in
  let edge = Gvecdb.create_edge db "knows" alice bob in
  (* Set properties *)
  Gvecdb.set_edge_props_capnp db edge "knows"
    (fun b -> SchemaBuilder.Builder.Knows.context_set b "test")
    SchemaBuilder.Builder.Knows.init_root
    SchemaBuilder.Builder.Knows.to_message;
  (* Verify metadata preserved *)
  match Gvecdb.get_edge_info db edge with
  | Some info ->
    check int64 "src preserved" alice info.src;
    check int64 "dst preserved" bob info.dst;
    check string "type preserved" "knows" info.edge_type
  | None -> fail "edge metadata lost!"

(** {1 Multiple types tests} *)

let test_multiple_node_types () =
  with_temp_db "basic" @@ fun db ->
  let person1 = Gvecdb.create_node db "person" in
  let person2 = Gvecdb.create_node db "person" in
  let doc1 = Gvecdb.create_node db "document" in
  match Gvecdb.get_node_info db person1, 
        Gvecdb.get_node_info db person2,
        Gvecdb.get_node_info db doc1 with
  | Some p1, Some p2, Some d1 ->
    check string "p1 type" "person" p1.node_type;
    check string "p2 type" "person" p2.node_type;
    check string "d1 type" "document" d1.node_type
  | _ -> fail "node info missing"

let test_multiple_edge_types () =
  with_temp_db "basic" @@ fun db ->
  let a = Gvecdb.create_node db "person" in
  let b = Gvecdb.create_node db "person" in
  let e1 = Gvecdb.create_edge db "knows" a b in
  let e2 = Gvecdb.create_edge db "likes" a b in
  let e3 = Gvecdb.create_edge db "follows" a b in
  match Gvecdb.get_edge_info db e1,
        Gvecdb.get_edge_info db e2,
        Gvecdb.get_edge_info db e3 with
  | Some i1, Some i2, Some i3 ->
    check string "e1 type" "knows" i1.edge_type;
    check string "e2 type" "likes" i2.edge_type;
    check string "e3 type" "follows" i3.edge_type
  | _ -> fail "edge info missing"

(** {1 ID allocation tests} *)

let test_node_ids_sequential () =
  with_temp_db "basic" @@ fun db ->
  let n1 = Gvecdb.create_node db "test" in
  let n2 = Gvecdb.create_node db "test" in
  let n3 = Gvecdb.create_node db "test" in
  check int64 "n2 = n1 + 1" (Int64.add n1 1L) n2;
  check int64 "n3 = n2 + 1" (Int64.add n2 1L) n3

let test_edge_ids_sequential () =
  with_temp_db "basic" @@ fun db ->
  let n = Gvecdb.create_node db "test" in
  let e1 = Gvecdb.create_edge db "rel" n n in
  let e2 = Gvecdb.create_edge db "rel" n n in
  let e3 = Gvecdb.create_edge db "rel" n n in
  check int64 "e2 = e1 + 1" (Int64.add e1 1L) e2;
  check int64 "e3 = e2 + 1" (Int64.add e2 1L) e3

(** {1 Persistence tests} *)

let test_persistence_across_reopen () =
  let path = temp_db_path "persist" in
  (try Sys.remove path with _ -> ());
  let alice_id, bob_id, edge_id =
    let db = Gvecdb.create path in
    register_schemas db;
    let alice = create_person db "Alice" 30 "alice@test.com" "Engineer" in
    let bob = create_person db "Bob" 25 "bob@test.com" "Designer" in
    let edge = create_knows_edge db alice bob 1234567890L "Work" 0.8 in
    Gvecdb.close db;
    (alice, bob, edge)
  in
  (* Reopen and verify data persisted *)
  Fun.protect ~finally:(fun () -> try Sys.remove path with _ -> ()) (fun () ->
    let db = Gvecdb.create path in
    Fun.protect ~finally:(fun () -> Gvecdb.close db) (fun () ->
      (* Check nodes exist *)
      check bool "alice exists" true (Gvecdb.node_exists db alice_id);
      check bool "bob exists" true (Gvecdb.node_exists db bob_id);
      (* Check node info *)
      (match Gvecdb.get_node_info db alice_id with
       | Some info -> check string "alice type" "person" info.node_type
       | None -> fail "alice info not found");
      (* Check node properties *)
      check string "alice name" "Alice" (get_person_name db alice_id);
      check string "bob name" "Bob" (get_person_name db bob_id);
      (* Check edge exists *)
      check bool "edge exists" true (Gvecdb.edge_exists db edge_id);
      (* Check edge info *)
      (match Gvecdb.get_edge_info db edge_id with
       | Some info ->
         check int64 "edge src" alice_id info.src;
         check int64 "edge dst" bob_id info.dst;
         check string "edge type" "knows" info.edge_type
       | None -> fail "edge info not found");
      (* Check adjacency indexes *)
      let outbound = Gvecdb.get_outbound_edges db alice_id in
      check int "outbound count" 1 (List.length outbound);
      let inbound = Gvecdb.get_inbound_edges db bob_id in
      check int "inbound count" 1 (List.length inbound)
    )
  )

(** {1 Test runner} *)

let node_tests = [
  "create_node", `Quick, test_create_node;
  "node_info", `Quick, test_node_info;
  "delete_node", `Quick, test_delete_node;
  "node_not_found", `Quick, test_node_not_found;
  "delete_nonexistent_node", `Quick, test_delete_nonexistent_node;
  "delete_node_cascades_edges", `Quick, test_delete_node_cascades_edges;
]

let edge_tests = [
  "create_edge", `Quick, test_create_edge;
  "edge_info", `Quick, test_edge_info;
  "self_edge", `Quick, test_self_edge;
  "delete_edge", `Quick, test_delete_edge;
  "delete_edge_cleans_adjacency", `Quick, test_delete_edge_cleans_adjacency;
]

let prop_tests = [
  "node_props_roundtrip", `Quick, test_node_props_roundtrip;
  "edge_props_roundtrip", `Quick, test_edge_props_roundtrip;
  "update_node_props", `Quick, test_update_node_props;
  "edge_meta_preserved", `Quick, test_edge_meta_preserved_after_props_update;
]

let type_tests = [
  "multiple_node_types", `Quick, test_multiple_node_types;
  "multiple_edge_types", `Quick, test_multiple_edge_types;
]

let id_tests = [
  "node_ids_sequential", `Quick, test_node_ids_sequential;
  "edge_ids_sequential", `Quick, test_edge_ids_sequential;
]

let persistence_tests = [
  "persistence_across_reopen", `Quick, test_persistence_across_reopen;
]

let () =
  run "Basic" [
    "nodes", node_tests;
    "edges", edge_tests;
    "properties", prop_tests;
    "types", type_tests;
    "ids", id_tests;
    "persistence", persistence_tests;
  ]

