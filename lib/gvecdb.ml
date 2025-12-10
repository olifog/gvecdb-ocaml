(** main API for gvecdb *)

include Types

module Bigstring = Bigstringaf

module Bigstring_message = Bigstring_message

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

let get_node_props_capnp db ?txn node_id of_message read_fn =
  Props_capnp.get_node_props_capnp db ?txn node_id of_message read_fn

(** set/get edge properties with capnproto *)
let set_edge_props_capnp db ?txn edge_id type_name build_fn init_root to_message =
  Props_capnp.set_edge_props_capnp db ?txn edge_id type_name build_fn init_root to_message

let get_edge_props_capnp db ?txn edge_id of_message read_fn =
  Props_capnp.get_edge_props_capnp db ?txn edge_id of_message read_fn

(** {1 edge/node info retrieval} *)

(** get edge info *)
let get_edge_info (db : t) ?txn (edge_id : edge_id) : edge_info option =
  match Props_capnp.get_edge_meta db ?txn edge_id with
  | None -> None
  | Some (intern_id, src, dst) ->
      let edge_type = Store.unintern db ?txn intern_id in
      Some { id = edge_id; edge_type; src; dst }

(** get node info *)
let get_node_info (db : t) ?txn (node_id : node_id) : node_info option =
  match Props_capnp.get_node_meta db ?txn node_id with
  | None -> None
  | Some intern_id ->
      let node_type = Store.unintern db ?txn intern_id in
      Some { id = node_id; node_type }

(** {1 node operations} *)

(** create a new node with the given type *)
let create_node (db : t) ?txn (node_type : string) : node_id =
  let intern_id = Store.intern db ?txn node_type in
  let node_id = Store.get_next_id db ?txn Metadata.next_node_id in
  let key = Keys.encode_id_bs node_id in
  Lmdb.Map.set db.node_meta ?txn key (Keys.encode_id_bs intern_id);
  Lmdb.Map.set db.nodes ?txn key Store.empty_bigstring;
  node_id

(** check if a node exists *)
let node_exists (db : t) ?txn (node_id : node_id) : bool =
  try
    let _ = Lmdb.Map.get db.node_meta ?txn (Keys.encode_id_bs node_id) in
    true
  with Not_found -> false

(** delete a node *)
let delete_node (db : t) ?txn (node_id : node_id) : unit =
  let key = Keys.encode_id_bs node_id in
  try
    Lmdb.Map.remove db.node_meta ?txn key;
    Lmdb.Map.remove db.nodes ?txn key
  with Not_found -> ()

(** {1 edge operations} *)

(** create a new edge with the given type, source and destination nodes *)
let create_edge (db : t) ?txn (edge_type : string) (src : node_id) (dst : node_id) : edge_id =
  let intern_id = Store.intern db ?txn edge_type in
  let edge_id = Store.get_next_id db ?txn Metadata.next_edge_id in
  let key = Keys.encode_id_bs edge_id in
  Lmdb.Map.set db.edge_meta ?txn key (Keys.encode_edge_meta ~type_id:intern_id ~src ~dst);
  Lmdb.Map.set db.edges ?txn key Store.empty_bigstring;
  let outbound_key = Keys.encode_adjacency_bs ~node_id:src ~intern_id ~opposite_id:dst ~edge_id in
  let inbound_key = Keys.encode_adjacency_bs ~node_id:dst ~intern_id ~opposite_id:src ~edge_id in
  Lmdb.Map.set db.outbound ?txn outbound_key Store.empty_bigstring;
  Lmdb.Map.set db.inbound ?txn inbound_key Store.empty_bigstring;
  edge_id

(** check if an edge exists *)
let edge_exists (db : t) ?txn (edge_id : edge_id) : bool =
  try
    let _ = Lmdb.Map.get db.edge_meta ?txn (Keys.encode_id_bs edge_id) in
    true
  with Not_found -> false

(** delete an edge *)
let delete_edge (db : t) ?txn (edge_id : edge_id) : unit =
  let key = Keys.encode_id_bs edge_id in
  try
    let meta = Lmdb.Map.get db.edge_meta ?txn key in
    let (intern_id, src, dst) = Keys.decode_edge_meta meta in
    Lmdb.Map.remove db.edges ?txn key;
    Lmdb.Map.remove db.edge_meta ?txn key;
    let outbound_key = Keys.encode_adjacency_bs ~node_id:src ~intern_id ~opposite_id:dst ~edge_id in
    let inbound_key = Keys.encode_adjacency_bs ~node_id:dst ~intern_id ~opposite_id:src ~edge_id in
    Lmdb.Map.remove db.outbound ?txn outbound_key;
    Lmdb.Map.remove db.inbound ?txn inbound_key
  with Not_found -> ()

(** {1 adjacency queries} *)

type direction = Outbound | Inbound

(** scan the adjacency index for edges with the given prefix *)
let scan_adjacency_index (db : t) ?txn ~direction ~node_id (map : (bigstring, bigstring, [`Uni]) Lmdb.Map.t) (prefix : bigstring) : edge_info list =
  let prefix_len = Bigstring.length prefix in
  let txn_ro = Option.map (fun t -> (t :> [`Read] Lmdb.Txn.t)) txn in
  let tuples = Lmdb.Cursor.go Lmdb.Ro ?txn:txn_ro map (fun cursor ->
    let rec collect acc =
      try
        let (key, _) = Lmdb.Cursor.next cursor in
        if Bigstring.length key >= prefix_len && Keys.bigstring_has_prefix ~prefix key then
          let (_, intern_id, opposite_id, edge_id) = Keys.decode_adjacency_bs key in
          collect ((edge_id, intern_id, opposite_id) :: acc)
        else
          acc
      with Lmdb.Not_found -> acc
    in
    try
      let (key, _) = Lmdb.Cursor.seek_range cursor prefix in
      if Bigstring.length key >= prefix_len && Keys.bigstring_has_prefix ~prefix key then
        let (_, intern_id, opposite_id, edge_id) = Keys.decode_adjacency_bs key in
        List.rev (collect [(edge_id, intern_id, opposite_id)])
      else
        []
    with Lmdb.Not_found -> []
  ) in
  List.map (fun (edge_id, intern_id, opposite_id) ->
    let edge_type = Store.unintern db ?txn intern_id in
    let (src, dst) = match direction with
      | Outbound -> (node_id, opposite_id)
      | Inbound -> (opposite_id, node_id)
    in
    { id = edge_id; edge_type; src; dst }
  ) tuples

(** get all outbound edges from a node *)
let get_outbound_edges (db : t) ?txn (node_id : node_id) : edge_info list =
  let prefix = Keys.encode_adjacency_prefix_bs ~node_id () in
  scan_adjacency_index db ?txn ~direction:Outbound ~node_id db.outbound prefix

(** get all inbound edges to a node *)
let get_inbound_edges (db : t) ?txn (node_id : node_id) : edge_info list =
  let prefix = Keys.encode_adjacency_prefix_bs ~node_id () in
  scan_adjacency_index db ?txn ~direction:Inbound ~node_id db.inbound prefix

(** get all outbound edges of a specific type from a node *)
let get_outbound_edges_by_type (db : t) ?txn (node_id : node_id) (edge_type : string) : edge_info list =
  match Store.lookup_intern db ?txn edge_type with
  | None -> []  (* type not interned means no edges of this type exist *)
  | Some intern_id ->
      let prefix = Keys.encode_adjacency_prefix_bs ~node_id ~intern_id () in
      scan_adjacency_index db ?txn ~direction:Outbound ~node_id db.outbound prefix

(** get all inbound edges of a specific type to a node *)
let get_inbound_edges_by_type (db : t) ?txn (node_id : node_id) (edge_type : string) : edge_info list =
  match Store.lookup_intern db ?txn edge_type with
  | None -> []  (* type not interned means no edges of this type exist *)
  | Some intern_id ->
      let prefix = Keys.encode_adjacency_prefix_bs ~node_id ~intern_id () in
      scan_adjacency_index db ?txn ~direction:Inbound ~node_id db.inbound prefix
