(** property operations using capnproto *)

open Types

module WrapperMod = Gvecdb_internal.Make(Capnp.BytesMessage)

(** capnproto serialization utilities *)
module CapnpCodec = struct
  (** serialize a capnproto message to bytes *)
  let serialize (message : 'a Capnp.BytesMessage.Message.t) : string =
    Capnp.Codecs.serialize_fold message
      ~compression:`None
      ~init:""
      ~f:(fun acc fragment -> acc ^ fragment)
  
  (** deserialize bytes to capnproto message *)
  let deserialize (bytes : string) : Capnp.Message.ro Capnp.BytesMessage.Message.t =
    let stream = Capnp.Codecs.FramedStream.of_string ~compression:`None bytes in
    match Capnp.Codecs.FramedStream.get_next_frame stream with
    | Ok msg -> Capnp.BytesMessage.Message.readonly msg
    | Error Capnp.Codecs.FramingError.Incomplete -> 
        failwith "incomplete capnproto frame"
    | Error Capnp.Codecs.FramingError.Unsupported -> 
        failwith "unsupported capnproto frame"
end

(** schema kinds for lmdb storage *)
type schema_kind = 
  | NodeSchemaKind
  | EdgeSchemaKind

(** register a node schema *)
let register_node_schema (db : t) ?txn (type_name : string) (schema_id : int64) : unit =
  let key = "schema:" ^ type_name in
  let kind_byte = '\x00' in
  let schema_id_bytes = Keys.encode_id schema_id in
  let value = String.make 1 kind_byte ^ schema_id_bytes in
  Lmdb.Map.set db.metadata ?txn key value

(** register an edge schema *)
let register_edge_schema (db : t) ?txn (type_name : string) (schema_id : int64) : unit =
  let key = "schema:" ^ type_name in
  let kind_byte = '\x01' in
  let schema_id_bytes = Keys.encode_id schema_id in
  let value = String.make 1 kind_byte ^ schema_id_bytes in
  Lmdb.Map.set db.metadata ?txn key value

(** get schema metadata from lmdb *)
let get_schema_metadata (db : t) ?txn (type_name : string) : (schema_kind * int64) option =
  let key = "schema:" ^ type_name in
  try
    let value = Lmdb.Map.get db.metadata ?txn key in
    if String.length value < 9 then None
    else
      let kind = match value.[0] with
        | '\x00' -> NodeSchemaKind
        | '\x01' -> EdgeSchemaKind
        | _ -> failwith "invalid schema kind byte"
      in
      let schema_id_bytes = String.sub value 1 8 in
      let schema_id = Keys.decode_id schema_id_bytes in
      Some (kind, schema_id)
  with Not_found -> None

(** set node properties with metadata wrapper *)
let set_node_props_capnp
    (db : t)
    ?txn
    (node_id : node_id)
    (type_name : string)
    (build_fn : 'builder -> unit)
    (init_root : unit -> 'builder)
    (to_message : 'builder -> 'a Capnp.BytesMessage.Message.t) : unit =
  
  let intern_id = Store.intern db ?txn type_name in
  
  let user_builder = init_root () in
  build_fn user_builder;
  let user_message = to_message user_builder in
  let user_blob = CapnpCodec.serialize user_message in
  
  let wrapper_builder = WrapperMod.Builder.NodePropertyBlob.init_root () in
  WrapperMod.Builder.NodePropertyBlob.version_set_int_exn wrapper_builder 1;
  WrapperMod.Builder.NodePropertyBlob.type_id_set wrapper_builder (Stdint.Uint64.of_int64 intern_id);
  WrapperMod.Builder.NodePropertyBlob.properties_set wrapper_builder user_blob;
  
  let wrapper_message = WrapperMod.Builder.NodePropertyBlob.to_message wrapper_builder in
  let blob = CapnpCodec.serialize wrapper_message in
  
  let key = Keys.encode_id node_id in
  Lmdb.Map.set db.nodes ?txn key blob

(** get node properties with zero-copy from lmdb mmap *)
let get_node_props_capnp
    (db : t)
    ?txn
    (node_id : node_id)
    (_type_name : string)
    (of_message : Capnp.Message.ro Capnp.BytesMessage.Message.t -> 'reader)
    (read_fn : 'reader -> 'result) : 'result =
  
  let key = Keys.encode_id node_id in
  let blob = 
    try Lmdb.Map.get db.nodes ?txn key
    with Not_found ->
      failwith (Printf.sprintf "node not found: %Ld" node_id)
  in
  
  let wrapper_message = CapnpCodec.deserialize blob in
  let wrapper_reader = WrapperMod.Reader.NodePropertyBlob.of_message wrapper_message in
  let user_blob = WrapperMod.Reader.NodePropertyBlob.properties_get wrapper_reader in
  let user_message = CapnpCodec.deserialize user_blob in
  let user_reader = of_message user_message in
  
  read_fn user_reader

(** set edge properties with metadata wrapper *)
let set_edge_props_capnp
    (db : t)
    ?txn
    (edge_id : edge_id)
    (type_name : string)
    (build_fn : 'builder -> unit)
    (init_root : unit -> 'builder)
    (to_message : 'builder -> 'a Capnp.BytesMessage.Message.t) : unit =
  
  let blob = 
    try Lmdb.Map.get db.edges ?txn (Keys.encode_id edge_id)
    with Not_found -> failwith (Printf.sprintf "edge not found: %Ld" edge_id)
  in
  let wrapper_msg = CapnpCodec.deserialize blob in
  let wrapper = WrapperMod.Reader.EdgePropertyBlob.of_message wrapper_msg in
  let src = Stdint.Uint64.to_int64 (WrapperMod.Reader.EdgePropertyBlob.src_get wrapper) in
  let dst = Stdint.Uint64.to_int64 (WrapperMod.Reader.EdgePropertyBlob.dst_get wrapper) in
  
  let new_intern_id = Store.intern db ?txn type_name in
  
  let user_builder = init_root () in
  build_fn user_builder;
  let user_message = to_message user_builder in
  let user_blob = CapnpCodec.serialize user_message in
  
  let wrapper_builder = WrapperMod.Builder.EdgePropertyBlob.init_root () in
  WrapperMod.Builder.EdgePropertyBlob.version_set_int_exn wrapper_builder 1;
  WrapperMod.Builder.EdgePropertyBlob.type_id_set wrapper_builder (Stdint.Uint64.of_int64 new_intern_id);
  WrapperMod.Builder.EdgePropertyBlob.src_set wrapper_builder (Stdint.Uint64.of_int64 src);
  WrapperMod.Builder.EdgePropertyBlob.dst_set wrapper_builder (Stdint.Uint64.of_int64 dst);
  WrapperMod.Builder.EdgePropertyBlob.properties_set wrapper_builder user_blob;
  
  let wrapper_message = WrapperMod.Builder.EdgePropertyBlob.to_message wrapper_builder in
  let blob = CapnpCodec.serialize wrapper_message in
  Lmdb.Map.set db.edges ?txn (Keys.encode_id edge_id) blob

(** get edge properties with zero-copy from lmdb mmap *)
let get_edge_props_capnp
    (db : t)
    ?txn
    (edge_id : edge_id)
    (_type_name : string)
    (of_message : Capnp.Message.ro Capnp.BytesMessage.Message.t -> 'reader)
    (read_fn : 'reader -> 'result) : 'result =
  
  let key = Keys.encode_id edge_id in
  let blob =
    try Lmdb.Map.get db.edges ?txn key
    with Not_found ->
      failwith (Printf.sprintf "edge not found: %Ld" edge_id)
  in
  
  let wrapper_message = CapnpCodec.deserialize blob in
  let wrapper_reader = WrapperMod.Reader.EdgePropertyBlob.of_message wrapper_message in
  let user_blob = WrapperMod.Reader.EdgePropertyBlob.properties_get wrapper_reader in
  let user_message = CapnpCodec.deserialize user_blob in
  let user_reader = of_message user_message in
  
  read_fn user_reader
