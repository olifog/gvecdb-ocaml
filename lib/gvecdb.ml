include Types
module Bigstring = Bigstringaf
module Bigstring_message = Bigstring_message

let create ?map_size path = Store.create ?map_size path
let close = Store.close

let register_node_schema_capnp db ?txn type_name schema_id =
  Props_capnp.register_node_schema db ?txn type_name schema_id

let register_edge_schema_capnp db ?txn type_name schema_id =
  Props_capnp.register_edge_schema db ?txn type_name schema_id

let set_node_props_capnp db ?txn node_id type_name build_fn init_root to_message
    =
  Props_capnp.set_node_props_capnp db ?txn node_id type_name build_fn init_root
    to_message

let get_node_props_capnp db ?txn node_id of_message read_fn =
  Props_capnp.get_node_props_capnp db ?txn node_id of_message read_fn

let set_edge_props_capnp db ?txn edge_id type_name build_fn init_root to_message
    =
  Props_capnp.set_edge_props_capnp db ?txn edge_id type_name build_fn init_root
    to_message

let get_edge_props_capnp db ?txn edge_id of_message read_fn =
  Props_capnp.get_edge_props_capnp db ?txn edge_id of_message read_fn

let get_edge_info (db : t) ?txn (edge_id : edge_id) : (edge_info, error) result
    =
  let* intern_id, src, dst = Props_capnp.get_edge_meta db ?txn edge_id in
  try
    let edge_type = Store.unintern db ?txn intern_id in
    Ok { id = edge_id; edge_type; src; dst }
  with Not_found | Lmdb.Not_found ->
    Error (Corrupted_data "edge type intern_id not found in reverse lookup")

let get_node_info (db : t) ?txn (node_id : node_id) : (node_info, error) result
    =
  let* intern_id = Props_capnp.get_node_meta db ?txn node_id in
  try
    let node_type = Store.unintern db ?txn intern_id in
    Ok { id = node_id; node_type }
  with Not_found | Lmdb.Not_found ->
    Error (Corrupted_data "node type intern_id not found in reverse lookup")

let create_node (db : t) ?txn (node_type : string) : (node_id, error) result =
  let* intern_id = Store.intern db ?txn node_type in
  wrap_lmdb_exn (fun () ->
      let node_id = Store.get_next_id db ?txn Metadata.next_node_id in
      let key = Keys.encode_id_bs node_id in
      Lmdb.Map.set db.node_meta ?txn key (Keys.encode_id_bs intern_id);
      Lmdb.Map.set db.nodes ?txn key Store.empty_bigstring;
      node_id)

let node_exists (db : t) ?txn (node_id : node_id) : (bool, error) result =
  try
    let _ = Lmdb.Map.get db.node_meta ?txn (Keys.encode_id_bs node_id) in
    Ok true
  with
  | Not_found | Lmdb.Not_found -> Ok false
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

let create_edge (db : t) ?txn (edge_type : string) (src : node_id)
    (dst : node_id) : (edge_id, error) result =
  let* intern_id = Store.intern db ?txn edge_type in
  wrap_lmdb_exn (fun () ->
      let edge_id = Store.get_next_id db ?txn Metadata.next_edge_id in
      let key = Keys.encode_id_bs edge_id in
      Lmdb.Map.set db.edge_meta ?txn key
        (Keys.encode_edge_meta ~type_id:intern_id ~src ~dst);
      Lmdb.Map.set db.edges ?txn key Store.empty_bigstring;
      let outbound_key =
        Keys.encode_adjacency_bs ~node_id:src ~intern_id ~opposite_id:dst
          ~edge_id
      in
      let inbound_key =
        Keys.encode_adjacency_bs ~node_id:dst ~intern_id ~opposite_id:src
          ~edge_id
      in
      Lmdb.Map.set db.outbound ?txn outbound_key Store.empty_bigstring;
      Lmdb.Map.set db.inbound ?txn inbound_key Store.empty_bigstring;
      edge_id)

let edge_exists (db : t) ?txn (edge_id : edge_id) : (bool, error) result =
  try
    let _ = Lmdb.Map.get db.edge_meta ?txn (Keys.encode_id_bs edge_id) in
    Ok true
  with
  | Not_found | Lmdb.Not_found -> Ok false
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

(* Delete edge data without cascade deleting vectors. Used by delete_node to
   avoid redundant vector lookups. *)
let delete_edge_data (db : t) ?txn (edge_id : edge_id) : (unit, error) result =
  let key = Keys.encode_id_bs edge_id in
  try
    let meta = Lmdb.Map.get db.edge_meta ?txn key in
    let intern_id, src, dst = Keys.decode_edge_meta meta in
    Lmdb.Map.remove db.edges ?txn key;
    Lmdb.Map.remove db.edge_meta ?txn key;
    let outbound_key =
      Keys.encode_adjacency_bs ~node_id:src ~intern_id ~opposite_id:dst ~edge_id
    in
    let inbound_key =
      Keys.encode_adjacency_bs ~node_id:dst ~intern_id ~opposite_id:src ~edge_id
    in
    Lmdb.Map.remove db.outbound ?txn outbound_key;
    Lmdb.Map.remove db.inbound ?txn inbound_key;
    Ok ()
  with
  | Not_found | Lmdb.Not_found -> Error (Edge_not_found edge_id)
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))
  | Invalid_argument msg -> Error (Corrupted_data msg)

type direction = Outbound | Inbound

let scan_adjacency_index (db : t) ?txn ~direction ~node_id
    (map : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t) (prefix : bigstring) :
    (edge_info list, error) result =
  let prefix_len = Bigstring.length prefix in
  let txn_ro = Option.map (fun t -> (t :> [ `Read ] Lmdb.Txn.t)) txn in
  try
    let tuples =
      Lmdb.Cursor.go Lmdb.Ro ?txn:txn_ro map (fun cursor ->
          let rec collect acc =
            try
              let key, _ = Lmdb.Cursor.next cursor in
              if
                Bigstring.length key >= prefix_len
                && Keys.bigstring_has_prefix ~prefix key
              then
                let _, intern_id, opposite_id, edge_id =
                  Keys.decode_adjacency_bs key
                in
                collect ((edge_id, intern_id, opposite_id) :: acc)
              else acc
            with Lmdb.Not_found -> acc
          in
          try
            let key, _ = Lmdb.Cursor.seek_range cursor prefix in
            if
              Bigstring.length key >= prefix_len
              && Keys.bigstring_has_prefix ~prefix key
            then
              let _, intern_id, opposite_id, edge_id =
                Keys.decode_adjacency_bs key
              in
              List.rev (collect [ (edge_id, intern_id, opposite_id) ])
            else []
          with Lmdb.Not_found -> [])
    in
    Ok
      (List.map
         (fun (edge_id, intern_id, opposite_id) ->
           let edge_type = Store.unintern db ?txn intern_id in
           let src, dst =
             match direction with
             | Outbound -> (node_id, opposite_id)
             | Inbound -> (opposite_id, node_id)
           in
           { id = edge_id; edge_type; src; dst })
         tuples)
  with
  | Not_found | Lmdb.Not_found ->
      Error (Corrupted_data "intern_id not found in reverse lookup")
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))
  | Invalid_argument msg -> Error (Corrupted_data msg)

let get_outbound_edges (db : t) ?txn (node_id : node_id) :
    (edge_info list, error) result =
  let prefix = Keys.encode_adjacency_prefix_bs ~node_id () in
  scan_adjacency_index db ?txn ~direction:Outbound ~node_id db.outbound prefix

let get_inbound_edges (db : t) ?txn (node_id : node_id) :
    (edge_info list, error) result =
  let prefix = Keys.encode_adjacency_prefix_bs ~node_id () in
  scan_adjacency_index db ?txn ~direction:Inbound ~node_id db.inbound prefix

let get_outbound_edges_by_type (db : t) ?txn (node_id : node_id)
    (edge_type : string) : (edge_info list, error) result =
  match Store.lookup_intern db ?txn edge_type with
  | None -> Ok [] (* type not interned means no edges of this type exist *)
  | Some intern_id ->
      let prefix = Keys.encode_adjacency_prefix_bs ~node_id ~intern_id () in
      scan_adjacency_index db ?txn ~direction:Outbound ~node_id db.outbound
        prefix

let get_inbound_edges_by_type (db : t) ?txn (node_id : node_id)
    (edge_type : string) : (edge_info list, error) result =
  match Store.lookup_intern db ?txn edge_type with
  | None -> Ok [] (* type not interned means no edges of this type exist *)
  | Some intern_id ->
      let prefix = Keys.encode_adjacency_prefix_bs ~node_id ~intern_id () in
      scan_adjacency_index db ?txn ~direction:Inbound ~node_id db.inbound prefix

(* Create a vector attached to an owner (node or edge) with a tag. Requires
   explicit transaction for thread-safety of vector file allocation.
   If ~normalize is true (default), normalizes vector for efficient cosine sim *)
let create_vector_internal (db : t) ~txn ~normalize (owner_kind : owner_kind)
    (owner_id : id) (vector_tag : string) (data : bigstring) :
    (vector_id, error) result =
  let* vector_tag_id = Store.intern db ~txn vector_tag in
  let dim = Float32_vec.dim data in
  let store_data, norm =
    if normalize then Float32_vec.normalize data
    else (data, sqrt (Float32_vec.norm_sq data))
  in
  match Vector_file.allocate db.vector_file dim with
  | Error e -> Error (Storage_error (Vector_file.error_to_string e))
  | Ok file_offset -> (
      match
        Vector_file.write_vector_at db.vector_file file_offset
          ~normalized:normalize store_data norm
      with
      | Error e -> Error (Storage_error (Vector_file.error_to_string e))
      | Ok () ->
          wrap_lmdb_exn (fun () ->
              let vector_id =
                Store.get_next_id db ~txn Metadata.next_vector_id
              in
              let key = Keys.encode_id_bs vector_id in
              let index_key =
                Keys.encode_vector_index_bs ~owner_kind ~owner_id ~vector_tag_id
                  ~vector_id
              in
              Lmdb.Map.set db.vector_index ~txn index_key Store.empty_bigstring;
              let owner_value =
                Keys.encode_vector_owner_bs ~owner_kind ~owner_id ~vector_tag_id
                  ~file_offset
              in
              Lmdb.Map.set db.vector_owners ~txn key owner_value;
              vector_id))

let create_vector (db : t) ~txn ?(normalize = true) (node_id : node_id)
    (vector_tag : string) (data : bigstring) : (vector_id, error) result =
  let* exists = node_exists db ~txn node_id in
  if not exists then Error (Node_not_found node_id)
  else create_vector_internal db ~txn ~normalize Node node_id vector_tag data

let create_edge_vector (db : t) ~txn ?(normalize = true) (edge_id : edge_id)
    (vector_tag : string) (data : bigstring) : (vector_id, error) result =
  let* exists = edge_exists db ~txn edge_id in
  if not exists then Error (Edge_not_found edge_id)
  else create_vector_internal db ~txn ~normalize Edge edge_id vector_tag data

let vector_exists (db : t) ?txn (vector_id : vector_id) : (bool, error) result =
  try
    let _ = Lmdb.Map.get db.vector_owners ?txn (Keys.encode_id_bs vector_id) in
    Ok true
  with
  | Not_found | Lmdb.Not_found -> Ok false
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

let get_vector (db : t) ?txn (vector_id : vector_id) : (bigstring, error) result
    =
  let key = Keys.encode_id_bs vector_id in
  try
    let owner_bs = Lmdb.Map.get db.vector_owners ?txn key in
    let _, _, _, file_offset = Keys.decode_vector_owner_bs owner_bs in
    match Vector_file.read_vector_at db.vector_file file_offset with
    | Ok data -> Ok data
    | Error e -> Error (Storage_error (Vector_file.error_to_string e))
  with
  | Not_found | Lmdb.Not_found -> Error (Vector_not_found vector_id)
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

let get_vector_info (db : t) ?txn (vector_id : vector_id) :
    (vector_info, error) result =
  let key = Keys.encode_id_bs vector_id in
  let owner_bs =
    try Some (Lmdb.Map.get db.vector_owners ?txn key)
    with Not_found | Lmdb.Not_found -> None
  in
  match owner_bs with
  | None -> Error (Vector_not_found vector_id)
  | Some owner_bs -> (
      try
        let owner_kind, owner_id, vector_tag_id, _file_offset =
          Keys.decode_vector_owner_bs owner_bs
        in
        let vector_tag = Store.unintern db ?txn vector_tag_id in
        Ok { vector_id; owner_kind; owner_id; vector_tag }
      with
      | Not_found | Lmdb.Not_found ->
          Error
            (Corrupted_data "vector tag intern_id not found in reverse lookup")
      | Lmdb.Map_full -> Error Storage_full
      | Lmdb.Error code ->
          Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code)))

(* Delete a single vector by ID. Used by both public delete_vector and cascade
   deletes *)
let delete_vector_internal (db : t) ?txn (vector_id : vector_id) :
    (unit, error) result =
  let key = Keys.encode_id_bs vector_id in
  let owner_bs =
    try Some (Lmdb.Map.get db.vector_owners ?txn key)
    with Not_found | Lmdb.Not_found -> None
  in
  match owner_bs with
  | None -> Error (Vector_not_found vector_id)
  | Some owner_bs -> (
      try
        let owner_kind, owner_id, vector_tag_id, _file_offset =
          Keys.decode_vector_owner_bs owner_bs
        in
        Lmdb.Map.remove db.vector_owners ?txn key;
        let index_key =
          Keys.encode_vector_index_bs ~owner_kind ~owner_id ~vector_tag_id
            ~vector_id
        in
        Lmdb.Map.remove db.vector_index ?txn index_key;
        Ok ()
      with
      | Not_found | Lmdb.Not_found ->
          Error (Corrupted_data "vector index entry missing during delete")
      | Lmdb.Map_full -> Error Storage_full
      | Lmdb.Error code ->
          Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code)))

let delete_vector (db : t) ~txn (vector_id : vector_id) : (unit, error) result =
  delete_vector_internal db ~txn vector_id

(* Delete all vectors attached to an owner (node or edge). Used for cascade
   deletes. Silently ignores already-deleted vectors. *)
let delete_vectors_for_owner (db : t) ?txn (owner_kind : owner_kind)
    (owner_id : id) : (unit, error) result =
  let prefix = Keys.encode_vector_index_prefix_bs ~owner_kind ~owner_id () in
  let prefix_len = Bigstring.length prefix in
  let txn_ro = Option.map (fun t -> (t :> [ `Read ] Lmdb.Txn.t)) txn in
  try
    (* first, collect all vector IDs for this owner *)
    let vector_ids =
      Lmdb.Cursor.go Lmdb.Ro ?txn:txn_ro db.vector_index (fun cursor ->
          let rec collect acc =
            try
              let key, _ = Lmdb.Cursor.next cursor in
              if
                Bigstring.length key >= prefix_len
                && Keys.bigstring_has_prefix ~prefix key
              then
                let _, _, _, vid = Keys.decode_vector_index_bs key in
                collect (vid :: acc)
              else acc
            with Lmdb.Not_found -> acc
          in
          try
            let key, _ = Lmdb.Cursor.seek_range cursor prefix in
            if
              Bigstring.length key >= prefix_len
              && Keys.bigstring_has_prefix ~prefix key
            then
              let _, _, _, vid = Keys.decode_vector_index_bs key in
              collect [ vid ]
            else []
          with Lmdb.Not_found -> [])
    in
    (* then delete each vector *)
    let rec delete_all = function
      | [] -> Ok ()
      | vid :: rest -> (
          match delete_vector_internal db ?txn vid with
          | Ok () -> delete_all rest
          | Error (Vector_not_found _) -> delete_all rest (* already deleted *)
          | Error e -> Error e)
    in
    delete_all vector_ids
  with
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

let delete_edge (db : t) ?txn (edge_id : edge_id) : (unit, error) result =
  let* () = delete_vectors_for_owner db ?txn Edge edge_id in
  delete_edge_data db ?txn edge_id

let delete_node (db : t) ?txn (node_id : node_id) : (unit, error) result =
  let key = Keys.encode_id_bs node_id in
  (* check node exists first *)
  let node_meta_exists =
    try
      let _ = Lmdb.Map.get db.node_meta ?txn key in
      true
    with Not_found | Lmdb.Not_found -> false
  in
  if not node_meta_exists then Error (Node_not_found node_id)
  else
    try
      (* 1. Delete all vectors directly attached to this node *)
      let* () = delete_vectors_for_owner db ?txn Node node_id in
      (* 2. Get all outbound edges and delete them (with their vectors) *)
      let* outbound_edges = get_outbound_edges db ?txn node_id in
      let rec delete_edges = function
        | [] -> Ok ()
        | edge :: rest ->
            let* () = delete_vectors_for_owner db ?txn Edge edge.id in
            let* () = delete_edge_data db ?txn edge.id in
            delete_edges rest
      in
      let* () = delete_edges outbound_edges in
      (* 3. Get all inbound edges and delete them (with their vectors) *)
      let* inbound_edges = get_inbound_edges db ?txn node_id in
      let* () = delete_edges inbound_edges in
      (* 4. Delete the node itself *)
      Lmdb.Map.remove db.nodes ?txn key;
      Lmdb.Map.remove db.node_meta ?txn key;
      Ok ()
    with
    | Lmdb.Map_full -> Error Storage_full
    | Lmdb.Error code ->
        Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

(* Get all vectors for an owner (node or edge), optionally filtered by tag *)
let get_vectors_for_owner_internal (db : t) ?txn (owner_kind : owner_kind)
    (owner_id : id) ?vector_tag () : (vector_info list, error) result =
  let vector_tag_id_opt =
    match vector_tag with
    | None -> None
    | Some tag -> Store.lookup_intern db ?txn tag
  in
  match (vector_tag, vector_tag_id_opt) with
  | Some _, None -> Ok []
  | _ -> (
      let prefix =
        Keys.encode_vector_index_prefix_bs ~owner_kind ~owner_id
          ?vector_tag_id:vector_tag_id_opt ()
      in
      let prefix_len = Bigstring.length prefix in
      let txn_ro = Option.map (fun t -> (t :> [ `Read ] Lmdb.Txn.t)) txn in
      try
        let results =
          Lmdb.Cursor.go Lmdb.Ro ?txn:txn_ro db.vector_index (fun cursor ->
              let rec collect acc =
                try
                  let key, _ = Lmdb.Cursor.next cursor in
                  if
                    Bigstring.length key >= prefix_len
                    && Keys.bigstring_has_prefix ~prefix key
                  then
                    let _, _, tag_id, vid = Keys.decode_vector_index_bs key in
                    collect ((vid, tag_id) :: acc)
                  else acc
                with Lmdb.Not_found -> acc
              in
              try
                let key, _ = Lmdb.Cursor.seek_range cursor prefix in
                if
                  Bigstring.length key >= prefix_len
                  && Keys.bigstring_has_prefix ~prefix key
                then
                  let _, _, tag_id, vid = Keys.decode_vector_index_bs key in
                  List.rev (collect [ (vid, tag_id) ])
                else []
              with Lmdb.Not_found -> [])
        in
        let rec map_with_unintern acc = function
          | [] -> Ok (List.rev acc)
          | (vid, tag_id) :: rest -> (
              try
                let vtag = Store.unintern db ?txn tag_id in
                map_with_unintern
                  ({ vector_id = vid; owner_kind; owner_id; vector_tag = vtag }
                  :: acc)
                  rest
              with Not_found | Lmdb.Not_found ->
                Error
                  (Corrupted_data
                     "vector tag intern_id not found in reverse lookup"))
        in
        map_with_unintern [] results
      with
      | Lmdb.Map_full -> Error Storage_full
      | Lmdb.Error code ->
          Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code)))

let get_vectors_for_node (db : t) ?txn (node_id : node_id) ?vector_tag () :
    (vector_info list, error) result =
  get_vectors_for_owner_internal db ?txn Node node_id ?vector_tag ()

let get_vectors_for_edge (db : t) ?txn (edge_id : edge_id) ?vector_tag () :
    (vector_info list, error) result =
  get_vectors_for_owner_internal db ?txn Edge edge_id ?vector_tag ()

let knn_brute_force = Knn.brute_force
let knn_brute_force_bs = Knn.brute_force_bs
