module SchemaBuilder = Schemas.Make (Capnp.BytesMessage)

let ok_exn = function
  | Ok x -> x
  | Error e -> Alcotest.fail (Gvecdb.Error.to_string e)

let find_capnp_path () =
  let candidates =
    [
      "test_schemas/schemas.capnp";
      "../test_schemas/schemas.capnp";
      "../../test_schemas/schemas.capnp";
    ]
  in
  match List.find_opt Sys.file_exists candidates with
  | Some p -> p
  | None -> Alcotest.fail "cannot find test_schemas/schemas.capnp"

let register_all_schemas db =
  let path = find_capnp_path () in
  ignore
    (ok_exn
       (Gvecdb.register_schema_from_capnp db
          ~kind:Gvecdb.Schema_registry.NodeSchemaKind ~type_name:"person"
          ~capnp_path:path ~struct_name:"Person" ()));
  ignore
    (ok_exn
       (Gvecdb.register_schema_from_capnp db
          ~kind:Gvecdb.Schema_registry.EdgeSchemaKind ~type_name:"knows"
          ~capnp_path:path ~struct_name:"Knows" ()))

let with_db f =
  Test_common.with_temp_db "schema" (fun db ->
      register_all_schemas db;
      f db)

let test_schema_registration () =
  Test_common.with_temp_db "schema_reg" (fun db ->
      let path = find_capnp_path () in
      let person =
        ok_exn
          (Gvecdb.register_schema_from_capnp db
             ~kind:Gvecdb.Schema_registry.NodeSchemaKind ~type_name:"person"
             ~capnp_path:path ~struct_name:"Person" ())
      in
      Alcotest.(check int) "person fields" 4 (List.length person.fields);
      Alcotest.(check int) "person data words" 1 person.data_word_count;
      Alcotest.(check int) "person pointers" 3 person.pointer_count;
      let knows =
        ok_exn
          (Gvecdb.register_schema_from_capnp db
             ~kind:Gvecdb.Schema_registry.EdgeSchemaKind ~type_name:"knows"
             ~capnp_path:path ~struct_name:"Knows" ())
      in
      Alcotest.(check int) "knows fields" 4 (List.length knows.fields);
      let open Gvecdb.Schema_registry in
      let fields =
        [
          {
            name = "x";
            field_type = Float64;
            offset = 0;
            is_pointer = false;
            default_value = No_default;
          };
          {
            name = "label";
            field_type = Text;
            offset = 0;
            is_pointer = true;
            default_value = No_default;
          };
        ]
      in
      let point =
        ok_exn
          (Gvecdb.register_schema_from_fields db ~kind:NodeSchemaKind
             ~type_name:"point" ~data_word_count:1 ~pointer_count:1 ~fields ())
      in
      Alcotest.(check int) "point fields" 2 (List.length point.fields))

let test_schema_persistence () =
  let path = Test_common.temp_db_path "schema_persist" in
  Test_common.cleanup_db_files path;
  Fun.protect
    ~finally:(fun () -> Test_common.cleanup_db_files path)
    (fun () ->
      let db = ok_exn (Gvecdb.create path) in
      let _ =
        ok_exn
          (Gvecdb.register_schema_from_capnp db
             ~kind:Gvecdb.Schema_registry.NodeSchemaKind ~type_name:"person"
             ~capnp_path:(find_capnp_path ()) ~struct_name:"Person" ())
      in
      Gvecdb.close db;
      let db2 = ok_exn (Gvecdb.create path) in
      Gvecdb.load_all_schemas db2;
      let schema = ok_exn (Gvecdb.get_schema db2 "person") in
      Alcotest.(check int) "persisted" 4 (List.length schema.fields);
      Gvecdb.close db2)

let test_dynamic_reader () =
  with_db (fun db ->
      let alice =
        Test_common.create_person db "Alice" 30 "alice@test.com" "A bio"
      in
      let bob = Test_common.create_person db "Bob" 25 "b@c.d" "" in
      let edge = Test_common.create_knows_edge db alice bob 2020L "work" 0.8 in
      (match ok_exn (Gvecdb.read_node_field db alice "age") with
      | V_uint32 age -> Alcotest.(check int32) "age" 30l age
      | _ -> Alcotest.fail "expected V_uint32");
      (match ok_exn (Gvecdb.read_node_field db alice "name") with
      | V_text name -> Alcotest.(check string) "name" "Alice" name
      | _ -> Alcotest.fail "expected V_text");
      (match ok_exn (Gvecdb.read_node_field db alice "email") with
      | V_text s -> Alcotest.(check string) "email" "alice@test.com" s
      | _ -> Alcotest.fail "expected V_text");
      (match ok_exn (Gvecdb.read_node_field db alice "bio") with
      | V_text s -> Alcotest.(check string) "bio" "A bio" s
      | _ -> Alcotest.fail "expected V_text");
      (match ok_exn (Gvecdb.read_edge_field db edge "since") with
      | V_int64 since -> Alcotest.(check int64) "since" 2020L since
      | _ -> Alcotest.fail "expected V_int64");
      (match ok_exn (Gvecdb.read_edge_field db edge "strength") with
      | V_float32 s ->
          Alcotest.(check bool) "strength" true (Float.abs (s -. 0.8) < 0.01)
      | _ -> Alcotest.fail "expected V_float32");
      (match Gvecdb.read_node_field db alice "nonexistent" with
      | Error _ -> ()
      | Ok _ -> Alcotest.fail "expected error for unknown field");
      match Gvecdb.read_node_field db 99999L "name" with
      | Error (Gvecdb.Node_not_found _) -> ()
      | _ -> Alcotest.fail "expected Node_not_found")

let test_filter_operators () =
  with_db (fun db ->
      let alice = Test_common.create_person db "Alice" 30 "" "" in
      let bob = Test_common.create_person db "Bob" 25 "" "" in
      let charlie = Test_common.create_person db "Charlie" 35 "" "" in
      let dave = Test_common.create_person db "Dave" 40 "" "" in
      let _ = Test_common.create_knows_edge db alice bob 2020L "work" 0.9 in
      let _ =
        Test_common.create_knows_edge db alice charlie 2015L "school" 0.3
      in
      let _ = Test_common.create_knows_edge db alice dave 2022L "" 0.2 in
      let filter field_name op value =
        [ Gvecdb.Filter.{ field_name; op; value } ]
      in
      let count filters =
        List.length (ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()))
      in
      Alcotest.(check int)
        "eq 2020" 1
        (count (filter "since" Eq (V_int64 2020L)));
      Alcotest.(check int)
        "gte 2020" 2
        (count (filter "since" Gte (V_int64 2020L)));
      Alcotest.(check int)
        "lt 2018" 1
        (count (filter "since" Lt (V_int64 2018L)));
      Alcotest.(check int)
        "lte 2015" 1
        (count (filter "since" Lte (V_int64 2015L)));
      Alcotest.(check int)
        "gt 0.5" 1
        (count (filter "strength" Gt (V_float32 0.5)));
      Alcotest.(check int)
        "neq work" 2
        (count (filter "context" Neq (V_text "work")));
      Alcotest.(check int)
        "text eq" 1
        (count (filter "context" Eq (V_text "work")));
      Alcotest.(check int)
        "empty filters" 3
        (List.length
           (ok_exn (Gvecdb.get_outbound_edges db alice ~filters:[] ()))))

let test_filter_and () =
  with_db (fun db ->
      let alice = Test_common.create_person db "Alice" 30 "" "" in
      let bob = Test_common.create_person db "Bob" 25 "" "" in
      let charlie = Test_common.create_person db "Charlie" 35 "" "" in
      let dave = Test_common.create_person db "Dave" 40 "" "" in
      let _ = Test_common.create_knows_edge db alice bob 2020L "" 0.9 in
      let _ = Test_common.create_knows_edge db alice charlie 2015L "" 0.8 in
      let _ = Test_common.create_knows_edge db alice dave 2022L "" 0.2 in
      let filters =
        Gvecdb.Filter.
          [
            { field_name = "since"; op = Gte; value = V_int64 2018L };
            { field_name = "strength"; op = Gt; value = V_float32 0.5 };
          ]
      in
      let edges = ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()) in
      Alcotest.(check int) "AND filter" 1 (List.length edges);
      Alcotest.(check int64) "dst is bob" bob (List.hd edges).dst)

let test_filter_with_edge_type () =
  with_db (fun db ->
      let alice = Test_common.create_person db "Alice" 30 "" "" in
      let bob = Test_common.create_person db "Bob" 25 "" "" in
      let charlie = Test_common.create_person db "Charlie" 35 "" "" in
      let _ = Test_common.create_knows_edge db alice bob 2020L "" 0.9 in
      let _ = Test_common.create_knows_edge db alice charlie 2015L "" 0.3 in
      let filters =
        [
          Gvecdb.Filter.
            { field_name = "since"; op = Gte; value = V_int64 2018L };
        ]
      in
      Alcotest.(check int)
        "type+filter" 1
        (List.length
           (ok_exn
              (Gvecdb.get_outbound_edges db alice ~edge_type:"knows" ~filters ())));
      let _ = Test_common.create_knows_edge db bob alice 2020L "" 0.9 in
      let _ = Test_common.create_knows_edge db charlie alice 2015L "" 0.3 in
      let inbound_filters =
        [
          Gvecdb.Filter.
            { field_name = "strength"; op = Gte; value = V_float32 0.5 };
        ]
      in
      Alcotest.(check int)
        "inbound filter" 1
        (List.length
           (ok_exn
              (Gvecdb.get_inbound_edges db alice ~filters:inbound_filters ()))))

let test_filter_no_props () =
  with_db (fun db ->
      let alice = Test_common.create_person db "Alice" 30 "" "" in
      let bob = Test_common.create_person db "Bob" 25 "" "" in
      let _ = ok_exn (Gvecdb.create_edge db "knows" alice bob) in
      let filters =
        [
          Gvecdb.Filter.{ field_name = "since"; op = Eq; value = V_int64 2020L };
        ]
      in
      Alcotest.(check int)
        "no match on unset" 0
        (List.length (ok_exn (Gvecdb.get_outbound_edges db alice ~filters ()))))

let () =
  Alcotest.run "Schema & Filter"
    [
      ( "schema",
        [
          Alcotest.test_case "registration" `Quick test_schema_registration;
          Alcotest.test_case "persistence" `Quick test_schema_persistence;
        ] );
      ( "dynamic_reader",
        [ Alcotest.test_case "read_fields" `Quick test_dynamic_reader ] );
      ( "filter",
        [
          Alcotest.test_case "operators" `Quick test_filter_operators;
          Alcotest.test_case "and" `Quick test_filter_and;
          Alcotest.test_case "with_edge_type" `Quick test_filter_with_edge_type;
          Alcotest.test_case "no_props" `Quick test_filter_no_props;
        ] );
    ]
