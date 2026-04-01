module SchemaBuilder = Schemas.Make (Capnp.BytesMessage)

let ok_exn = function
  | Ok x -> x
  | Error e -> Alcotest.fail (Gvecdb.Error.to_string e)

let find_capnp_path () =
  let candidates = [
    "test_schemas/schemas.capnp";
    "../test_schemas/schemas.capnp";
    "../../test_schemas/schemas.capnp";
  ] in
  match List.find_opt Sys.file_exists candidates with
  | Some p -> p
  | None -> Alcotest.fail "cannot find test_schemas/schemas.capnp"

let register_all_schemas db =
  let path = find_capnp_path () in
  ignore (ok_exn (Gvecdb.register_schema_from_capnp db
      ~kind:Gvecdb.Schema_registry.NodeSchemaKind
      ~type_name:"person" ~capnp_path:path ~struct_name:"Person" ()));
  ignore (ok_exn (Gvecdb.register_schema_from_capnp db
      ~kind:Gvecdb.Schema_registry.EdgeSchemaKind
      ~type_name:"knows" ~capnp_path:path ~struct_name:"Knows" ()))

let with_db f =
  Test_common.with_temp_db "schema" (fun db ->
    register_all_schemas db;
    f db)

(* -- Schema Registration Tests -- *)

let test_register_person_schema () =
  Test_common.with_temp_db "schema_reg" (fun db ->
    let schema = ok_exn (Gvecdb.register_schema_from_capnp db
        ~kind:Gvecdb.Schema_registry.NodeSchemaKind
        ~type_name:"person" ~capnp_path:(find_capnp_path ())
        ~struct_name:"Person" ()) in
    Alcotest.(check int) "data word count" 1 schema.data_word_count;
    Alcotest.(check int) "pointer count" 3 schema.pointer_count;
    Alcotest.(check int) "field count" 4 (List.length schema.fields);
    let names = List.map (fun (f : Gvecdb.Schema_registry.field_descriptor) ->
        f.name) schema.fields in
    Alcotest.(check bool) "has name" true (List.mem "name" names);
    Alcotest.(check bool) "has age" true (List.mem "age" names);
    Alcotest.(check bool) "has email" true (List.mem "email" names);
    Alcotest.(check bool) "has bio" true (List.mem "bio" names))

let test_register_knows_schema () =
  Test_common.with_temp_db "schema_reg" (fun db ->
    let schema = ok_exn (Gvecdb.register_schema_from_capnp db
        ~kind:Gvecdb.Schema_registry.EdgeSchemaKind
        ~type_name:"knows" ~capnp_path:(find_capnp_path ())
        ~struct_name:"Knows" ()) in
    Alcotest.(check int) "data word count" 3 schema.data_word_count;
    Alcotest.(check int) "pointer count" 1 schema.pointer_count;
    Alcotest.(check int) "field count" 4 (List.length schema.fields))

let test_schema_persistence () =
  let path = Test_common.temp_db_path "schema_persist" in
  Test_common.cleanup_db_files path;
  Fun.protect ~finally:(fun () -> Test_common.cleanup_db_files path) (fun () ->
    let db = ok_exn (Gvecdb.create path) in
    let _ = ok_exn (Gvecdb.register_schema_from_capnp db
        ~kind:Gvecdb.Schema_registry.NodeSchemaKind
        ~type_name:"person" ~capnp_path:(find_capnp_path ())
        ~struct_name:"Person" ()) in
    Gvecdb.close db;
    let db2 = ok_exn (Gvecdb.create path) in
    Gvecdb.load_all_schemas db2;
    let schema = ok_exn (Gvecdb.get_schema db2 "person") in
    Alcotest.(check int) "field count after reopen" 4 (List.length schema.fields);
    Alcotest.(check int) "data word count" 1 schema.data_word_count;
    Gvecdb.close db2)

let test_register_from_fields () =
  Test_common.with_temp_db "schema_fields" (fun db ->
    let open Gvecdb.Schema_registry in
    let fields = [
      { name = "x"; field_type = Float64; offset = 0; is_pointer = false;
        default_value = No_default };
      { name = "y"; field_type = Float64; offset = 8; is_pointer = false;
        default_value = No_default };
      { name = "label"; field_type = Text; offset = 0; is_pointer = true;
        default_value = No_default };
    ] in
    let schema = ok_exn (Gvecdb.register_schema_from_fields db
        ~kind:NodeSchemaKind ~type_name:"point"
        ~data_word_count:2 ~pointer_count:1 ~fields ()) in
    Alcotest.(check int) "fields" 3 (List.length schema.fields);
    Alcotest.(check int) "data words" 2 schema.data_word_count)

(* -- Dynamic Reader Tests -- *)

let test_read_uint32_field () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "a@b.c" "bio" in
    match ok_exn (Gvecdb.read_node_field db alice "age") with
    | V_uint32 age -> Alcotest.(check int32) "age" 30l age
    | _ -> Alcotest.fail "expected V_uint32")

let test_read_text_field () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "a@b.c" "bio text" in
    match ok_exn (Gvecdb.read_node_field db alice "name") with
    | V_text name -> Alcotest.(check string) "name" "Alice" name
    | _ -> Alcotest.fail "expected V_text")

let test_read_edge_fields () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "a@b.c" "" in
    let bob = Test_common.create_person db "Bob" 25 "b@c.d" "" in
    let edge = Test_common.create_knows_edge db alice bob 2020L "work" 0.8 in
    (match ok_exn (Gvecdb.read_edge_field db edge "since") with
     | V_int64 since -> Alcotest.(check int64) "since" 2020L since
     | _ -> Alcotest.fail "expected V_int64");
    match ok_exn (Gvecdb.read_edge_field db edge "strength") with
    | V_float32 s ->
        Alcotest.(check bool) "strength ~0.8" true (Float.abs (s -. 0.8) < 0.01)
    | _ -> Alcotest.fail "expected V_float32")

let test_read_multiple_text_fields () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "alice@test.com" "A bio" in
    (match ok_exn (Gvecdb.read_node_field db alice "name") with
     | V_text s -> Alcotest.(check string) "name" "Alice" s
     | _ -> Alcotest.fail "expected V_text");
    (match ok_exn (Gvecdb.read_node_field db alice "email") with
     | V_text s -> Alcotest.(check string) "email" "alice@test.com" s
     | _ -> Alcotest.fail "expected V_text");
    match ok_exn (Gvecdb.read_node_field db alice "bio") with
    | V_text s -> Alcotest.(check string) "bio" "A bio" s
    | _ -> Alcotest.fail "expected V_text")

let test_read_unknown_field () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    match Gvecdb.read_node_field db alice "nonexistent" with
    | Error _ -> ()
    | Ok _ -> Alcotest.fail "expected error for unknown field")

let test_read_nonexistent_node () =
  with_db (fun db ->
    match Gvecdb.read_node_field db 99999L "name" with
    | Error (Gvecdb.Node_not_found _) -> ()
    | Error e -> Alcotest.fail ("wrong error: " ^ Gvecdb.Error.to_string e)
    | Ok _ -> Alcotest.fail "expected Node_not_found")

(* -- Filter Tests -- *)

let test_filter_int64_eq () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let charlie = Test_common.create_person db "Charlie" 35 "" "" in
    let _ = Test_common.create_knows_edge db alice bob 2020L "work" 0.9 in
    let _ = Test_common.create_knows_edge db alice charlie 2015L "school" 0.5 in
    let filters = [Gvecdb.Filter.{
      field_name = "since"; op = Eq;
      value = Gvecdb.Dynamic_reader.V_int64 2020L
    }] in
    let edges = ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()) in
    Alcotest.(check int) "one match" 1 (List.length edges);
    Alcotest.(check int64) "dst is bob" bob (List.hd edges).dst)

let test_filter_int64_gte () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let charlie = Test_common.create_person db "Charlie" 35 "" "" in
    let dave = Test_common.create_person db "Dave" 40 "" "" in
    let _ = Test_common.create_knows_edge db alice bob 2020L "" 0.0 in
    let _ = Test_common.create_knows_edge db alice charlie 2015L "" 0.0 in
    let _ = Test_common.create_knows_edge db alice dave 2022L "" 0.0 in
    let filters = [Gvecdb.Filter.{
      field_name = "since"; op = Gte;
      value = Gvecdb.Dynamic_reader.V_int64 2020L
    }] in
    let edges = ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()) in
    Alcotest.(check int) "two matches (2020, 2022)" 2 (List.length edges))

let test_filter_int64_lt () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let charlie = Test_common.create_person db "Charlie" 35 "" "" in
    let _ = Test_common.create_knows_edge db alice bob 2020L "" 0.0 in
    let _ = Test_common.create_knows_edge db alice charlie 2015L "" 0.0 in
    let filters = [Gvecdb.Filter.{
      field_name = "since"; op = Lt;
      value = Gvecdb.Dynamic_reader.V_int64 2018L
    }] in
    let edges = ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()) in
    Alcotest.(check int) "one match (2015)" 1 (List.length edges))

let test_filter_int64_lte () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let charlie = Test_common.create_person db "Charlie" 35 "" "" in
    let _ = Test_common.create_knows_edge db alice bob 2020L "" 0.0 in
    let _ = Test_common.create_knows_edge db alice charlie 2015L "" 0.0 in
    let filters = [Gvecdb.Filter.{
      field_name = "since"; op = Lte;
      value = Gvecdb.Dynamic_reader.V_int64 2015L
    }] in
    let edges = ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()) in
    Alcotest.(check int) "one match (2015)" 1 (List.length edges);
    Alcotest.(check int64) "dst is charlie" charlie (List.hd edges).dst)

let test_filter_float32_gt () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let charlie = Test_common.create_person db "Charlie" 35 "" "" in
    let _ = Test_common.create_knows_edge db alice bob 2020L "" 0.9 in
    let _ = Test_common.create_knows_edge db alice charlie 2015L "" 0.3 in
    let filters = [Gvecdb.Filter.{
      field_name = "strength"; op = Gt;
      value = Gvecdb.Dynamic_reader.V_float32 0.5
    }] in
    let edges = ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()) in
    Alcotest.(check int) "one strong edge" 1 (List.length edges))

let test_filter_neq () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let charlie = Test_common.create_person db "Charlie" 35 "" "" in
    let _ = Test_common.create_knows_edge db alice bob 2020L "work" 0.9 in
    let _ = Test_common.create_knows_edge db alice charlie 2015L "school" 0.5 in
    let filters = [Gvecdb.Filter.{
      field_name = "context"; op = Neq;
      value = Gvecdb.Dynamic_reader.V_text "work"
    }] in
    let edges = ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()) in
    Alcotest.(check int) "one non-work edge" 1 (List.length edges))

let test_filter_multiple_predicates () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let charlie = Test_common.create_person db "Charlie" 35 "" "" in
    let dave = Test_common.create_person db "Dave" 40 "" "" in
    let _ = Test_common.create_knows_edge db alice bob 2020L "" 0.9 in
    let _ = Test_common.create_knows_edge db alice charlie 2015L "" 0.8 in
    let _ = Test_common.create_knows_edge db alice dave 2022L "" 0.2 in
    let filters = Gvecdb.Filter.[
      { field_name = "since"; op = Gte;
        value = Gvecdb.Dynamic_reader.V_int64 2018L };
      { field_name = "strength"; op = Gt;
        value = Gvecdb.Dynamic_reader.V_float32 0.5 };
    ] in
    let edges = ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()) in
    Alcotest.(check int) "only bob (>=2018 AND >0.5)" 1 (List.length edges);
    Alcotest.(check int64) "dst is bob" bob (List.hd edges).dst)

let test_filter_empty_predicates () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let _ = Test_common.create_knows_edge db alice bob 2020L "" 0.9 in
    let edges = ok_exn (Gvecdb.get_outbound_edges db alice ~filters:[] ()) in
    Alcotest.(check int) "all edges returned" 1 (List.length edges))

let test_filter_text_eq () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let charlie = Test_common.create_person db "Charlie" 35 "" "" in
    let _ = Test_common.create_knows_edge db alice bob 2020L "work" 0.9 in
    let _ = Test_common.create_knows_edge db alice charlie 2015L "school" 0.5 in
    let filters = [Gvecdb.Filter.{
      field_name = "context"; op = Eq;
      value = Gvecdb.Dynamic_reader.V_text "work"
    }] in
    let edges = ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()) in
    Alcotest.(check int) "one work edge" 1 (List.length edges))

let test_filter_with_edge_type () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let charlie = Test_common.create_person db "Charlie" 35 "" "" in
    let _ = Test_common.create_knows_edge db alice bob 2020L "" 0.9 in
    let _ = Test_common.create_knows_edge db alice charlie 2015L "" 0.3 in
    let filters = [Gvecdb.Filter.{
      field_name = "since"; op = Gte;
      value = Gvecdb.Dynamic_reader.V_int64 2018L
    }] in
    let edges = ok_exn (Gvecdb.get_outbound_edges db alice
        ~edge_type:"knows" ~filters ()) in
    Alcotest.(check int) "filtered by type and predicate" 1
      (List.length edges))

let test_filter_inbound () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let charlie = Test_common.create_person db "Charlie" 35 "" "" in
    let _ = Test_common.create_knows_edge db bob alice 2020L "" 0.9 in
    let _ = Test_common.create_knows_edge db charlie alice 2015L "" 0.3 in
    let filters = [Gvecdb.Filter.{
      field_name = "strength"; op = Gte;
      value = Gvecdb.Dynamic_reader.V_float32 0.5
    }] in
    let edges = ok_exn (Gvecdb.get_inbound_edges db alice ~filters ()) in
    Alcotest.(check int) "one strong inbound" 1 (List.length edges))

let test_filter_edge_no_props () =
  with_db (fun db ->
    let alice = Test_common.create_person db "Alice" 30 "" "" in
    let bob = Test_common.create_person db "Bob" 25 "" "" in
    let _ = ok_exn (Gvecdb.create_edge db "knows" alice bob) in
    let filters = [Gvecdb.Filter.{
      field_name = "since"; op = Eq;
      value = Gvecdb.Dynamic_reader.V_int64 2020L
    }] in
    let edges = ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()) in
    Alcotest.(check int) "no match on unset props" 0 (List.length edges))

let () =
  Alcotest.run "Schema & Filter"
    [
      ( "schema_registration",
        [
          Alcotest.test_case "register person" `Quick
            test_register_person_schema;
          Alcotest.test_case "register knows" `Quick
            test_register_knows_schema;
          Alcotest.test_case "persistence" `Quick
            test_schema_persistence;
          Alcotest.test_case "from explicit fields" `Quick
            test_register_from_fields;
        ] );
      ( "dynamic_reader",
        [
          Alcotest.test_case "uint32" `Quick test_read_uint32_field;
          Alcotest.test_case "text" `Quick test_read_text_field;
          Alcotest.test_case "edge int64+float32" `Quick test_read_edge_fields;
          Alcotest.test_case "multiple text" `Quick
            test_read_multiple_text_fields;
          Alcotest.test_case "unknown field" `Quick test_read_unknown_field;
          Alcotest.test_case "nonexistent node" `Quick
            test_read_nonexistent_node;
        ] );
      ( "property_filter",
        [
          Alcotest.test_case "int64 eq" `Quick test_filter_int64_eq;
          Alcotest.test_case "int64 gte" `Quick test_filter_int64_gte;
          Alcotest.test_case "int64 lt" `Quick test_filter_int64_lt;
          Alcotest.test_case "int64 lte" `Quick test_filter_int64_lte;
          Alcotest.test_case "float32 gt" `Quick test_filter_float32_gt;
          Alcotest.test_case "neq" `Quick test_filter_neq;
          Alcotest.test_case "multiple AND" `Quick
            test_filter_multiple_predicates;
          Alcotest.test_case "empty predicates" `Quick
            test_filter_empty_predicates;
          Alcotest.test_case "text eq" `Quick test_filter_text_eq;
          Alcotest.test_case "with edge_type" `Quick
            test_filter_with_edge_type;
          Alcotest.test_case "inbound" `Quick test_filter_inbound;
          Alcotest.test_case "no props set" `Quick test_filter_edge_no_props;
        ] );
    ]
