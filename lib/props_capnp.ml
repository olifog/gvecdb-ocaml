(** Property storage using CapnProto serialization.
    
    Properties are stored as raw CapnProto wire format bytes.
    Metadata (type_id, src, dst) is stored separately in compact binary.
    
    Read path uses BigstringMessage with segments pointing directly into mmap.
    Zero-copy reads require the callback to run inside a transaction to ensure
    the mmap'd memory remains valid. *)

open Types

module Bigstring = Bigstringaf

(** Run a function with a read transaction. If a transaction is provided, use it.
    Otherwise create a new RO transaction that spans the entire callback.
    This ensures mmap'd memory remains valid for zero-copy reads. *)
let with_ro_txn (db : t) ?txn f =
  match txn with
  | Some txn -> f (txn :> [`Read] Lmdb.Txn.t)
  | None ->
    match Lmdb.Txn.go Lmdb.Ro db.env f with
    | Some result -> result
    | None -> failwith "read transaction aborted unexpectedly"

(** CapnProto wire format utilities. *)
module CapnpWireFormat = struct
  
  (** Serialize a builder message to wire format bytes. *)
  let to_bytes (message : 'a Capnp.BytesMessage.Message.t) : bigstring =
    let total_size = ref 0 in
    Capnp.Codecs.serialize_iter message ~compression:`None
      ~f:(fun fragment -> total_size := !total_size + String.length fragment);
    
    let buf = Bigstring.create !total_size in
    let offset = ref 0 in
    Capnp.Codecs.serialize_iter message ~compression:`None
      ~f:(fun fragment ->
        let len = String.length fragment in
        Bigstring.blit_from_string fragment ~src_off:0 buf ~dst_off:!offset ~len;
        offset := !offset + len);
    buf
  
  (** Parse wire format header. Returns (segment_sizes, data_offset). *)
  let parse_header (bs : bigstring) : int array * int =
    let segment_count = 1 + Int32.to_int (Bigstring.get_int32_le bs 0) in
    let segment_sizes = Array.make segment_count 0 in
    for i = 0 to segment_count - 1 do
      let word_count = Int32.to_int (Bigstring.get_int32_le bs (4 + i * 4)) in
      segment_sizes.(i) <- word_count * 8
    done;
    let header_words = (segment_count + 2) / 2 in
    let data_offset = header_words * 8 in
    (segment_sizes, data_offset)
  
  (** Create BigstringMessage with segments pointing into the source bigstring. *)
  let from_bytes (bs : bigstring) : Capnp.Message.ro Bigstring_message.Message.t =
    let (segment_sizes, data_offset) = parse_header bs in
    let segments = Array.make (Array.length segment_sizes) (Bigstring.create 0) in
    let offset = ref data_offset in
    for i = 0 to Array.length segment_sizes - 1 do
      let size = segment_sizes.(i) in
      segments.(i) <- Bigstring.sub bs ~off:!offset ~len:size;
      offset := !offset + size
    done;
    Bigstring_message.Message.readonly 
      (Bigstring_message.Message.of_storage (Array.to_list segments))
end

type schema_kind = 
  | NodeSchemaKind
  | EdgeSchemaKind

let register_node_schema (db : t) ?txn (type_name : string) (schema_id : int64) : unit =
  let key = "schema:" ^ type_name in
  let buf = Bigstring.create 9 in
  Bigstring.set buf 0 '\x00';
  Bigstring.set_int64_be buf 1 schema_id;
  Lmdb.Map.set db.metadata ?txn key buf

let register_edge_schema (db : t) ?txn (type_name : string) (schema_id : int64) : unit =
  let key = "schema:" ^ type_name in
  let buf = Bigstring.create 9 in
  Bigstring.set buf 0 '\x01';
  Bigstring.set_int64_be buf 1 schema_id;
  Lmdb.Map.set db.metadata ?txn key buf

let get_schema_metadata (db : t) ?txn (type_name : string) : (schema_kind * int64) option =
  let key = "schema:" ^ type_name in
  try
    let value = Lmdb.Map.get db.metadata ?txn key in
    if Bigstring.length value < 9 then None
    else
      let kind = match Bigstring.get value 0 with
        | '\x00' -> NodeSchemaKind
        | '\x01' -> EdgeSchemaKind
        | _ -> failwith "invalid schema kind byte"
      in
      let schema_id = Bigstring.get_int64_be value 1 in
      Some (kind, schema_id)
  with Not_found -> None

let set_node_props_capnp
    (db : t) ?txn (node_id : node_id) (type_name : string)
    (build_fn : 'builder -> unit)
    (init_root : unit -> 'builder)
    (to_message : 'builder -> 'a Capnp.BytesMessage.Message.t) : unit =
  let intern_id = Store.intern db ?txn type_name in
  let key = Keys.encode_id_bs node_id in
  Lmdb.Map.set db.node_meta ?txn key (Keys.encode_id_bs intern_id);
  let builder = init_root () in
  build_fn builder;
  let message = to_message builder in
  let bytes = CapnpWireFormat.to_bytes message in
  Lmdb.Map.set db.nodes ?txn key bytes

let get_node_props_capnp
    (db : t) ?txn (node_id : node_id)
    (of_message : Capnp.Message.ro Bigstring_message.Message.t -> 'reader)
    (read_fn : 'reader -> 'result) : 'result =
  with_ro_txn db ?txn (fun txn ->
    let key = Keys.encode_id_bs node_id in
    let bytes = 
      try Lmdb.Map.get db.nodes ~txn key
      with Not_found -> failwith (Printf.sprintf "node not found: %Ld" node_id)
    in
    let message = CapnpWireFormat.from_bytes bytes in
    read_fn (of_message message))

let set_edge_props_capnp
    (db : t) ?txn (edge_id : edge_id) (type_name : string)
    (build_fn : 'builder -> unit)
    (init_root : unit -> 'builder)
    (to_message : 'builder -> 'a Capnp.BytesMessage.Message.t) : unit =
  let key = Keys.encode_id_bs edge_id in
  let (_, src, dst) = 
    try 
      let meta = Lmdb.Map.get db.edge_meta ?txn key in
      Keys.decode_edge_meta meta
    with Not_found -> failwith (Printf.sprintf "edge not found: %Ld" edge_id)
  in
  let new_intern_id = Store.intern db ?txn type_name in
  Lmdb.Map.set db.edge_meta ?txn key (Keys.encode_edge_meta ~type_id:new_intern_id ~src ~dst);
  let builder = init_root () in
  build_fn builder;
  let message = to_message builder in
  let bytes = CapnpWireFormat.to_bytes message in
  Lmdb.Map.set db.edges ?txn key bytes

let get_edge_props_capnp
    (db : t) ?txn (edge_id : edge_id)
    (of_message : Capnp.Message.ro Bigstring_message.Message.t -> 'reader)
    (read_fn : 'reader -> 'result) : 'result =
  with_ro_txn db ?txn (fun txn ->
    let key = Keys.encode_id_bs edge_id in
    let bytes =
      try Lmdb.Map.get db.edges ~txn key
      with Not_found -> failwith (Printf.sprintf "edge not found: %Ld" edge_id)
    in
    let message = CapnpWireFormat.from_bytes bytes in
    read_fn (of_message message))

let get_edge_meta (db : t) ?txn (edge_id : edge_id) : (intern_id * node_id * node_id) option =
  let key = Keys.encode_id_bs edge_id in
  try
    let meta = Lmdb.Map.get db.edge_meta ?txn key in
    Some (Keys.decode_edge_meta meta)
  with Not_found -> None

let get_node_meta (db : t) ?txn (node_id : node_id) : intern_id option =
  let key = Keys.encode_id_bs node_id in
  try
    let meta = Lmdb.Map.get db.node_meta ?txn key in
    Some (Keys.decode_id_bs meta)
  with Not_found -> None
