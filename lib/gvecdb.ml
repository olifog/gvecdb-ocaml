(** main API for gvecdb *)

include Types

(** create or open a database at the given path *)
let create = Store.create

(** close the database *)
let close = Store.close

(** {1 transactions} *)

(** run a function within a read-write transaction *)
let with_transaction = with_transaction

(** run a function within a read-only transaction *)
let with_transaction_ro = with_transaction_ro

(** abort the current transaction *)
let abort_transaction = abort_transaction

(** {1 capnproto property operations} *)

(** register schemas *)
let register_node_schema_capnp db ?txn type_name schema_id =
  Props_capnp.register_node_schema db ?txn type_name schema_id
let register_edge_schema_capnp db ?txn type_name schema_id =
  Props_capnp.register_edge_schema db ?txn type_name schema_id

(** set/get node properties with capnproto *)
let set_node_props_capnp db ?txn node_id type_name build_fn init_root to_message =
  Props_capnp.set_node_props_capnp db ?txn node_id type_name build_fn init_root to_message
let get_node_props_capnp db ?txn node_id type_name of_message read_fn =
  Props_capnp.get_node_props_capnp db ?txn node_id type_name of_message read_fn

(** set/get edge properties with capnproto *)
let set_edge_props_capnp db ?txn edge_id type_name build_fn init_root to_message =
  Props_capnp.set_edge_props_capnp db ?txn edge_id type_name build_fn init_root to_message
let get_edge_props_capnp db ?txn edge_id type_name of_message read_fn =
  Props_capnp.get_edge_props_capnp db ?txn edge_id type_name of_message read_fn

(** parse edge data from wrapped format *)
let parse_edge_data (data : string) : (intern_id * node_id * node_id) =
  let module WrapperMod = Gvecdb_internal.Make(Capnp.BytesMessage) in
  let stream = Capnp.Codecs.FramedStream.of_string ~compression:`None data in
  match Capnp.Codecs.FramedStream.get_next_frame stream with
  | Ok msg ->
      let message = Capnp.BytesMessage.Message.readonly msg in
      let wrapper = WrapperMod.Reader.EdgePropertyBlob.of_message message in
      let tid = Stdint.Uint64.to_int64 (WrapperMod.Reader.EdgePropertyBlob.type_id_get wrapper) in
      let src = Stdint.Uint64.to_int64 (WrapperMod.Reader.EdgePropertyBlob.src_get wrapper) in
      let dst = Stdint.Uint64.to_int64 (WrapperMod.Reader.EdgePropertyBlob.dst_get wrapper) in
      (tid, src, dst)
  | Error _ ->
      failwith "failed to parse edge data"

(** get edge info by edge ID *)
let get_edge_info (db : t) ?txn (edge_id : edge_id) : edge_info option =
  try
    let edge_data = Lmdb.Map.get db.edges ?txn (Keys.encode_id edge_id) in
    let (intern_id, src, dst) = parse_edge_data edge_data in
    let edge_type = Store.unintern db ?txn intern_id in
    Some { id = edge_id; edge_type; src; dst }
  with Not_found -> None

(** get node info by node ID *)
let get_node_info (db : t) ?txn (node_id : node_id) : node_info option =
  try
    let key = Keys.encode_id node_id in
    let blob = Lmdb.Map.get db.nodes ?txn key in
    
    (* deserialize wrapper to extract type_id *)
    let module WrapperMod = Gvecdb_internal.Make(Capnp.BytesMessage) in
    let wrapper_message = Props_capnp.CapnpCodec.deserialize blob in
    let wrapper_reader = WrapperMod.Reader.NodePropertyBlob.of_message wrapper_message in
    let type_id = Stdint.Uint64.to_int64 (WrapperMod.Reader.NodePropertyBlob.type_id_get wrapper_reader) in
    let node_type = Store.unintern db ?txn type_id in
    
    Some { id = node_id; node_type }
  with Not_found -> None

(** create a new node with the given type *)
let create_node (db : t) ?txn (node_type : string) : node_id =
  let intern_id = Store.intern db ?txn node_type in
  let node_id = Store.get_next_id db ?txn Metadata.next_node_id in
  
  (* create empty wrapped blob with just metadata *)
  let module WrapperMod = Gvecdb_internal.Make(Capnp.BytesMessage) in
  let wrapper_builder = WrapperMod.Builder.NodePropertyBlob.init_root () in
  WrapperMod.Builder.NodePropertyBlob.version_set_int_exn wrapper_builder 1;
  WrapperMod.Builder.NodePropertyBlob.type_id_set wrapper_builder (Stdint.Uint64.of_int64 intern_id);
  WrapperMod.Builder.NodePropertyBlob.properties_set wrapper_builder "";
  
  let wrapper_message = WrapperMod.Builder.NodePropertyBlob.to_message wrapper_builder in
  let blob = Props_capnp.CapnpCodec.serialize wrapper_message in
  
  Lmdb.Map.set db.nodes ?txn (Keys.encode_id node_id) blob;
  node_id

(** create a new edge between two nodes *)
let create_edge (db : t) ?txn (edge_type : string) (src : node_id) (dst : node_id) : edge_id =
  let intern_id = Store.intern db ?txn edge_type in
  let edge_id = Store.get_next_id db ?txn Metadata.next_edge_id in
  
  (* create wrapped blob with metadata *)
  let module WrapperMod = Gvecdb_internal.Make(Capnp.BytesMessage) in
  let wrapper_builder = WrapperMod.Builder.EdgePropertyBlob.init_root () in
  WrapperMod.Builder.EdgePropertyBlob.version_set_int_exn wrapper_builder 1;
  WrapperMod.Builder.EdgePropertyBlob.type_id_set wrapper_builder (Stdint.Uint64.of_int64 intern_id);
  WrapperMod.Builder.EdgePropertyBlob.src_set wrapper_builder (Stdint.Uint64.of_int64 src);
  WrapperMod.Builder.EdgePropertyBlob.dst_set wrapper_builder (Stdint.Uint64.of_int64 dst);
  WrapperMod.Builder.EdgePropertyBlob.properties_set wrapper_builder "";
  
  let wrapper_message = WrapperMod.Builder.EdgePropertyBlob.to_message wrapper_builder in
  let blob = Props_capnp.CapnpCodec.serialize wrapper_message in
  
  Lmdb.Map.set db.edges ?txn (Keys.encode_id edge_id) blob;
  
  (* update adjacency indexes *)
  let outbound_key = Keys.encode_adjacency ~node_id:src ~intern_id 
    ~opposite_id:dst ~edge_id in
  let inbound_key = Keys.encode_adjacency ~node_id:dst ~intern_id 
    ~opposite_id:src ~edge_id in
  
  Lmdb.Map.set db.outbound ?txn outbound_key "";
  Lmdb.Map.set db.inbound ?txn inbound_key "";
  
  edge_id

(** scan an adjacency index with a prefix and collect edge IDs, then look up edge info *)
let scan_adjacency_index (db : t) ?txn (map : (string, string, [`Uni]) Lmdb.Map.t) (prefix : string) : edge_info list =
  let prefix_len = String.length prefix in
  (* coerce txn to read-only for cursor (safe since we only do reads) *)
  let txn_ro = Option.map (fun t -> (t :> [`Read] Lmdb.Txn.t)) txn in
  (* first, collect all edge IDs using cursor *)
  let edge_ids = Lmdb.Cursor.go Lmdb.Ro ?txn:txn_ro map (fun cursor ->
    let rec collect acc =
      try
        let (key, _) = Lmdb.Cursor.next cursor in
        if String.length key >= prefix_len && String.sub key 0 prefix_len = prefix then
          let (_, _, _, edge_id) = Keys.decode_adjacency key in
          collect (edge_id :: acc)
        else
          acc
      with Lmdb.Not_found -> acc
    in
    try
      let (key, _) = Lmdb.Cursor.seek_range cursor prefix in
      if String.length key >= prefix_len && String.sub key 0 prefix_len = prefix then
        let (_, _, _, edge_id) = Keys.decode_adjacency key in
        List.rev (collect [edge_id])
      else
        []
    with Lmdb.Not_found -> []
  ) in
  (* now look up edge info for each ID (after cursor is closed) *)
  List.filter_map (fun edge_id -> get_edge_info db ?txn edge_id) edge_ids

(** get all outbound edges from a node *)
let get_outbound_edges (db : t) ?txn (node_id : node_id) : edge_info list =
  let prefix = Keys.encode_adjacency_prefix ~node_id () in
  scan_adjacency_index db ?txn db.outbound prefix

(** get all inbound edges to a node *)
let get_inbound_edges (db : t) ?txn (node_id : node_id) : edge_info list =
  let prefix = Keys.encode_adjacency_prefix ~node_id () in
  scan_adjacency_index db ?txn db.inbound prefix

(** get all outbound edges of a specific type from a node *)
let get_outbound_edges_by_type (db : t) ?txn (node_id : node_id) (edge_type : string) : edge_info list =
  match Store.lookup_intern db ?txn edge_type with
  | None -> []  (* type not interned means no edges of this type exist *)
  | Some intern_id ->
      let prefix = Keys.encode_adjacency_prefix ~node_id ~intern_id () in
      scan_adjacency_index db ?txn db.outbound prefix

(** get all inbound edges of a specific type to a node *)
let get_inbound_edges_by_type (db : t) ?txn (node_id : node_id) (edge_type : string) : edge_info list =
  match Store.lookup_intern db ?txn edge_type with
  | None -> []  (* type not interned means no edges of this type exist *)
  | Some intern_id ->
      let prefix = Keys.encode_adjacency_prefix ~node_id ~intern_id () in
      scan_adjacency_index db ?txn db.inbound prefix

(** check if a node exists *)
let node_exists (db : t) ?txn (node_id : node_id) : bool =
  try
    let _ = Lmdb.Map.get db.nodes ?txn (Keys.encode_id node_id) in
    true
  with Not_found -> false

(** delete a node *)
let delete_node (db : t) ?txn (node_id : node_id) : unit =
  try
    Lmdb.Map.remove db.nodes ?txn (Keys.encode_id node_id)
  with Not_found -> ()  (* node doesn't exist, silently succeed *)

(** check if an edge exists *)
let edge_exists (db : t) ?txn (edge_id : edge_id) : bool =
  try
    let _ = Lmdb.Map.get db.edges ?txn (Keys.encode_id edge_id) in
    true
  with Not_found -> false

(** delete an edge and clean up adjacency indexes *)
let delete_edge (db : t) ?txn (edge_id : edge_id) : unit =
  try
    (* get edge data to extract src, dst, and type for adjacency cleanup *)
    let edge_data = Lmdb.Map.get db.edges ?txn (Keys.encode_id edge_id) in
    let (intern_id, src, dst) = parse_edge_data edge_data in
    
    (* remove from edges table *)
    Lmdb.Map.remove db.edges ?txn (Keys.encode_id edge_id);
    
    (* remove from adjacency indexes *)
    let outbound_key = Keys.encode_adjacency ~node_id:src ~intern_id 
      ~opposite_id:dst ~edge_id in
    let inbound_key = Keys.encode_adjacency ~node_id:dst ~intern_id 
      ~opposite_id:src ~edge_id in
    
    Lmdb.Map.remove db.outbound ?txn outbound_key;
    Lmdb.Map.remove db.inbound ?txn inbound_key
  with Not_found -> ()  (* edge doesn't exist, silently succeed *)
