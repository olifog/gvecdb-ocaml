module SchemaBuilder = Schemas.Make (Capnp.BytesMessage)

(* SchemaReader is an alias for backward-compat in tests that import it *)
module SchemaReader = SchemaBuilder

let ok_exn = function
  | Ok x -> x
  | Error e -> Alcotest.fail (Gvecdb.Error.to_string e)

let temp_db_path prefix =
  Filename.(
    concat (get_temp_dir_name ())
      (Printf.sprintf "%s_%d_%d.db" prefix (Unix.getpid ()) (Random.int 100000)))

let cleanup_db_files path =
  (try Sys.remove path with _ -> ());
  let base = Filename.remove_extension path in
  (try Sys.remove (base ^ ".vectors") with _ -> ());
  let hnsw_dir = base ^ ".hnsw" in
  try
    if Sys.file_exists hnsw_dir && Sys.is_directory hnsw_dir then begin
      Array.iter
        (fun f -> try Sys.remove (Filename.concat hnsw_dir f) with _ -> ())
        (Sys.readdir hnsw_dir);
      Unix.rmdir hnsw_dir
    end
  with _ -> ()

let with_temp_db prefix f =
  let path = temp_db_path prefix in
  cleanup_db_files path;
  let db = Gvecdb.create path |> ok_exn in
  Fun.protect
    ~finally:(fun () ->
      Gvecdb.close db;
      cleanup_db_files path)
    (fun () -> f db)

let find_capnp_path () =
  let candidates = [
    "test_schemas/schemas.capnp";
    "../test_schemas/schemas.capnp";
    "../../test_schemas/schemas.capnp";
  ] in
  match List.find_opt Sys.file_exists candidates with
  | Some p -> p
  | None -> Alcotest.fail "cannot find test_schemas/schemas.capnp"

let register_schemas db =
  let path = find_capnp_path () in
  ignore (ok_exn (Gvecdb.register_schema_from_capnp db
      ~kind:Gvecdb.Schema_registry.NodeSchemaKind
      ~type_name:"person" ~capnp_path:path ~struct_name:"Person" ()));
  ignore (ok_exn (Gvecdb.register_schema_from_capnp db
      ~kind:Gvecdb.Schema_registry.EdgeSchemaKind
      ~type_name:"knows" ~capnp_path:path ~struct_name:"Knows" ()))

(** Serialize a capnp builder message to a bigstring suitable for
    set_node_props / set_edge_props. Uses wire format with framing header. *)
let capnp_to_bigstring to_message builder =
  let msg = to_message builder in
  let total = ref 0 in
  Capnp.Codecs.serialize_iter msg ~compression:`None ~f:(fun fragment ->
      total := !total + String.length fragment);
  let bs = Bigstringaf.create !total in
  let pos = ref 0 in
  Capnp.Codecs.serialize_iter msg ~compression:`None ~f:(fun fragment ->
      let len = String.length fragment in
      Bigstringaf.blit_from_string fragment ~src_off:0 bs ~dst_off:!pos ~len;
      pos := !pos + len);
  bs

let create_person db ?txn name age email bio =
  let node = ok_exn (Gvecdb.create_node db ?txn "person") in
  let builder = SchemaBuilder.Builder.Person.init_root () in
  SchemaBuilder.Builder.Person.name_set builder name;
  SchemaBuilder.Builder.Person.age_set_int_exn builder age;
  SchemaBuilder.Builder.Person.email_set builder email;
  SchemaBuilder.Builder.Person.bio_set builder bio;
  let bs = capnp_to_bigstring SchemaBuilder.Builder.Person.to_message builder in
  ok_exn (Gvecdb.set_node_props db ?txn node "person" bs);
  node

let create_knows_edge db ?txn src dst since context strength =
  let edge = ok_exn (Gvecdb.create_edge db ?txn "knows" src dst) in
  let builder = SchemaBuilder.Builder.Knows.init_root () in
  SchemaBuilder.Builder.Knows.since_set builder since;
  SchemaBuilder.Builder.Knows.context_set builder context;
  SchemaBuilder.Builder.Knows.strength_set builder strength;
  let bs = capnp_to_bigstring SchemaBuilder.Builder.Knows.to_message builder in
  ok_exn (Gvecdb.set_edge_props db ?txn edge bs);
  edge

(** Decode capnp wire-format bytes into a BytesMessage and apply reader fns.
    Uses the BytesMessage-based reader (SchemaBuilder.Reader) since
    FramedStream returns BytesMessage. *)
let decode_props_capnp bs of_message read_fn =
  if Bigstringaf.length bs = 0 then
    Alcotest.fail "empty props"
  else
    let s = Bigstringaf.to_string bs in
    let stream = Capnp.Codecs.FramedStream.of_string ~compression:`None s in
    match Capnp.Codecs.FramedStream.get_next_frame stream with
    | Ok msg ->
        let ro_msg = Capnp.BytesMessage.Message.readonly msg in
        let reader = of_message ro_msg in
        read_fn reader
    | Error _ -> Alcotest.fail "failed to decode capnp wire format"

let read_node_props_capnp db ?txn node_id of_message read_fn =
  let bs = ok_exn (Gvecdb.get_node_props db ?txn node_id) in
  decode_props_capnp bs of_message read_fn

let read_edge_props_capnp db ?txn edge_id of_message read_fn =
  let bs = ok_exn (Gvecdb.get_edge_props db ?txn edge_id) in
  decode_props_capnp bs of_message read_fn

let get_person_name db ?txn node =
  read_node_props_capnp db ?txn node
    SchemaReader.Reader.Person.of_message SchemaReader.Reader.Person.name_get

let get_person_age db ?txn node =
  read_node_props_capnp db ?txn node
    SchemaReader.Reader.Person.of_message SchemaReader.Reader.Person.age_get
