(** Common test utilities and schema setup *)

module SchemaBuilder = Schemas.Make(Capnp.BytesMessage)
module SchemaReader = Schemas.Make(Gvecdb.Bigstring_message)

let temp_db_path prefix =
  Filename.(concat (get_temp_dir_name ())
    (Printf.sprintf "%s_%d_%d.db" prefix (Unix.getpid ()) (Random.int 100000)))

let with_temp_db prefix f =
  let path = temp_db_path prefix in
  (try Sys.remove path with _ -> ());
  let db = Gvecdb.create path in
  Fun.protect ~finally:(fun () ->
    Gvecdb.close db;
    (try Sys.remove path with _ -> ())
  ) (fun () -> f db)

let register_schemas db =
  Gvecdb.register_node_schema_capnp db "person" 0xd8e6e025e7838111L;
  Gvecdb.register_edge_schema_capnp db "knows" 0xd3c22e2de1d0b32bL

let create_person db ?txn name age email bio =
  let node = Gvecdb.create_node db ?txn "person" in
  Gvecdb.set_node_props_capnp db ?txn node "person"
    (fun b ->
      SchemaBuilder.Builder.Person.name_set b name;
      SchemaBuilder.Builder.Person.age_set_int_exn b age;
      SchemaBuilder.Builder.Person.email_set b email;
      SchemaBuilder.Builder.Person.bio_set b bio)
    SchemaBuilder.Builder.Person.init_root
    SchemaBuilder.Builder.Person.to_message;
  node

let create_knows_edge db ?txn src dst since context strength =
  let edge = Gvecdb.create_edge db ?txn "knows" src dst in
  Gvecdb.set_edge_props_capnp db ?txn edge "knows"
    (fun b ->
      SchemaBuilder.Builder.Knows.since_set b since;
      SchemaBuilder.Builder.Knows.context_set b context;
      SchemaBuilder.Builder.Knows.strength_set b strength)
    SchemaBuilder.Builder.Knows.init_root
    SchemaBuilder.Builder.Knows.to_message;
  edge

let get_person_name db ?txn node =
  Gvecdb.get_node_props_capnp db ?txn node
    SchemaReader.Reader.Person.of_message
    SchemaReader.Reader.Person.name_get

let get_person_age db ?txn node =
  Gvecdb.get_node_props_capnp db ?txn node
    SchemaReader.Reader.Person.of_message
    SchemaReader.Reader.Person.age_get

