(** main API for gvecdb *)

include Types

module Bigstring = Bigstringaf

module Bigstring_message = Bigstring_message

(** create or open a database at the given path *)
let create ?map_size path = Store.create ?map_size path

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
let get_edge_info (db : t) ?txn (edge_id : edge_id) : (edge_info, error) result =
  let* (intern_id, src, dst) = Props_capnp.get_edge_meta db ?txn edge_id in
  try
    let edge_type = Store.unintern db ?txn intern_id in
    Ok { id = edge_id; edge_type; src; dst }
  with Not_found | Lmdb.Not_found -> 
    Error (Corrupted_data "edge type intern_id not found in reverse lookup")

(** get node info *)
let get_node_info (db : t) ?txn (node_id : node_id) : (node_info, error) result =
  let* intern_id = Props_capnp.get_node_meta db ?txn node_id in
  try
    let node_type = Store.unintern db ?txn intern_id in
    Ok { id = node_id; node_type }
  with Not_found | Lmdb.Not_found ->
    Error (Corrupted_data "node type intern_id not found in reverse lookup")

(** {1 node operations} *)

(** create a new node with the given type *)
let create_node (db : t) ?txn (node_type : string) : (node_id, error) result =
  let* intern_id = Store.intern db ?txn node_type in
  wrap_lmdb_exn (fun () ->
    let node_id = Store.get_next_id db ?txn Metadata.next_node_id in
    let key = Keys.encode_id_bs node_id in
    Lmdb.Map.set db.node_meta ?txn key (Keys.encode_id_bs intern_id);
    Lmdb.Map.set db.nodes ?txn key Store.empty_bigstring;
    node_id)

(** check if a node exists *)
let node_exists (db : t) ?txn (node_id : node_id) : (bool, error) result =
  try
    let _ = Lmdb.Map.get db.node_meta ?txn (Keys.encode_id_bs node_id) in
    Ok true
  with 
  | Not_found | Lmdb.Not_found -> Ok false
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code -> Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

(** {1 edge operations} *)

(** create a new edge with the given type, source and destination nodes *)
let create_edge (db : t) ?txn (edge_type : string) (src : node_id) (dst : node_id) : (edge_id, error) result =
  let* intern_id = Store.intern db ?txn edge_type in
  wrap_lmdb_exn (fun () ->
    let edge_id = Store.get_next_id db ?txn Metadata.next_edge_id in
    let key = Keys.encode_id_bs edge_id in
    Lmdb.Map.set db.edge_meta ?txn key (Keys.encode_edge_meta ~type_id:intern_id ~src ~dst);
    Lmdb.Map.set db.edges ?txn key Store.empty_bigstring;
    let outbound_key = Keys.encode_adjacency_bs ~node_id:src ~intern_id ~opposite_id:dst ~edge_id in
    let inbound_key = Keys.encode_adjacency_bs ~node_id:dst ~intern_id ~opposite_id:src ~edge_id in
    Lmdb.Map.set db.outbound ?txn outbound_key Store.empty_bigstring;
    Lmdb.Map.set db.inbound ?txn inbound_key Store.empty_bigstring;
    edge_id)

(** check if an edge exists *)
let edge_exists (db : t) ?txn (edge_id : edge_id) : (bool, error) result =
  try
    let _ = Lmdb.Map.get db.edge_meta ?txn (Keys.encode_id_bs edge_id) in
    Ok true
  with 
  | Not_found | Lmdb.Not_found -> Ok false
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code -> Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

(** delete an edge *)
let delete_edge (db : t) ?txn (edge_id : edge_id) : (unit, error) result =
  let key = Keys.encode_id_bs edge_id in
  try
    let meta = Lmdb.Map.get db.edge_meta ?txn key in
    let (intern_id, src, dst) = Keys.decode_edge_meta meta in
    Lmdb.Map.remove db.edges ?txn key;
    Lmdb.Map.remove db.edge_meta ?txn key;
    let outbound_key = Keys.encode_adjacency_bs ~node_id:src ~intern_id ~opposite_id:dst ~edge_id in
    let inbound_key = Keys.encode_adjacency_bs ~node_id:dst ~intern_id ~opposite_id:src ~edge_id in
    Lmdb.Map.remove db.outbound ?txn outbound_key;
    Lmdb.Map.remove db.inbound ?txn inbound_key;
    Ok ()
  with 
  | Not_found | Lmdb.Not_found -> Error (Edge_not_found edge_id)
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code -> Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))
  | Invalid_argument msg -> Error (Corrupted_data msg)

(** {1 adjacency queries} *)

type direction = Outbound | Inbound

(** scan the adjacency index for edges with the given prefix *)
let scan_adjacency_index (db : t) ?txn ~direction ~node_id (map : (bigstring, bigstring, [`Uni]) Lmdb.Map.t) (prefix : bigstring) : (edge_info list, error) result =
  let prefix_len = Bigstring.length prefix in
  let txn_ro = Option.map (fun t -> (t :> [`Read] Lmdb.Txn.t)) txn in
  try
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
    Ok (List.map (fun (edge_id, intern_id, opposite_id) ->
      let edge_type = Store.unintern db ?txn intern_id in
      let (src, dst) = match direction with
        | Outbound -> (node_id, opposite_id)
        | Inbound -> (opposite_id, node_id)
      in
      { id = edge_id; edge_type; src; dst }
    ) tuples)
  with
  | Not_found | Lmdb.Not_found -> Error (Corrupted_data "intern_id not found in reverse lookup")
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code -> Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))
  | Invalid_argument msg -> Error (Corrupted_data msg)

(** get all outbound edges from a node *)
let get_outbound_edges (db : t) ?txn (node_id : node_id) : (edge_info list, error) result =
  let prefix = Keys.encode_adjacency_prefix_bs ~node_id () in
  scan_adjacency_index db ?txn ~direction:Outbound ~node_id db.outbound prefix

(** get all inbound edges to a node *)
let get_inbound_edges (db : t) ?txn (node_id : node_id) : (edge_info list, error) result =
  let prefix = Keys.encode_adjacency_prefix_bs ~node_id () in
  scan_adjacency_index db ?txn ~direction:Inbound ~node_id db.inbound prefix

(** get all outbound edges of a specific type from a node *)
let get_outbound_edges_by_type (db : t) ?txn (node_id : node_id) (edge_type : string) : (edge_info list, error) result =
  match Store.lookup_intern db ?txn edge_type with
  | None -> Ok []  (* type not interned means no edges of this type exist *)
  | Some intern_id ->
      let prefix = Keys.encode_adjacency_prefix_bs ~node_id ~intern_id () in
      scan_adjacency_index db ?txn ~direction:Outbound ~node_id db.outbound prefix

(** get all inbound edges of a specific type to a node *)
let get_inbound_edges_by_type (db : t) ?txn (node_id : node_id) (edge_type : string) : (edge_info list, error) result =
  match Store.lookup_intern db ?txn edge_type with
  | None -> Ok []  (* type not interned means no edges of this type exist *)
  | Some intern_id ->
      let prefix = Keys.encode_adjacency_prefix_bs ~node_id ~intern_id () in
      scan_adjacency_index db ?txn ~direction:Inbound ~node_id db.inbound prefix

(** {1 node deletion} *)

(** delete a node and cascade delete all connected edges *)
let delete_node (db : t) ?txn (node_id : node_id) : (unit, error) result =
  let key = Keys.encode_id_bs node_id in
  match node_exists db ?txn node_id with
  | Error e -> Error e
  | Ok false -> Error (Node_not_found node_id)
  | Ok true ->
    let* outbound_edges = get_outbound_edges db ?txn node_id in
    let* inbound_edges = get_inbound_edges db ?txn node_id in
    let rec delete_edges edges =
      match edges with
      | [] -> Ok ()
      | edge :: rest ->
        match delete_edge db ?txn edge.id with
        | Ok () -> delete_edges rest
        | Error (Edge_not_found _) -> delete_edges rest
        | Error e -> Error e
    in
    let* () = delete_edges outbound_edges in
    let* () = delete_edges inbound_edges in
    wrap_lmdb_exn (fun () ->
      Lmdb.Map.remove db.node_meta ?txn key;
      Lmdb.Map.remove db.nodes ?txn key)

(** {1 vector operations} *)

(** create a new vector attached to a node with a tag *)
let create_vector (db : t) ?txn (node_id : node_id) (vector_tag : string) (data : bigstring) : (vector_id, error) result =
  let* _exists = node_exists db ?txn node_id in
  if not _exists then Error (Node_not_found node_id)
  else
    let* vector_tag_id = Store.intern db ?txn vector_tag in
    wrap_lmdb_exn (fun () ->
      let vector_id = Store.get_next_id db ?txn Metadata.next_vector_id in
      let key = Keys.encode_id_bs vector_id in
      Lmdb.Map.set db.vectors ?txn key data;
      let index_key = Keys.encode_vector_index_bs ~node_id ~vector_tag_id ~vector_id in
      Lmdb.Map.set db.vector_index ?txn index_key Store.empty_bigstring;
      let owner_value = Keys.encode_vector_owner_bs ~node_id ~vector_tag_id in
      Lmdb.Map.set db.vector_owners ?txn key owner_value;
      vector_id)

(** check if a vector exists *)
let vector_exists (db : t) ?txn (vector_id : vector_id) : (bool, error) result =
  try
    let _ = Lmdb.Map.get db.vectors ?txn (Keys.encode_id_bs vector_id) in
    Ok true
  with 
  | Not_found | Lmdb.Not_found -> Ok false
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code -> Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

(** get vector data by ID - returns a view into mmap, valid only within transaction *)
let get_vector (db : t) ?txn (vector_id : vector_id) : (bigstring, error) result =
  try
    let data = Lmdb.Map.get db.vectors ?txn (Keys.encode_id_bs vector_id) in
    Ok data
  with 
  | Not_found | Lmdb.Not_found -> Error (Vector_not_found vector_id)
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code -> Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

(** get vector info (owning node and tag) *)
let get_vector_info (db : t) ?txn (vector_id : vector_id) : (vector_info, error) result =
  let key = Keys.encode_id_bs vector_id in
  let owner_bs = 
    try Some (Lmdb.Map.get db.vector_owners ?txn key)
    with Not_found | Lmdb.Not_found -> None
  in
  match owner_bs with
  | None -> Error (Vector_not_found vector_id)
  | Some owner_bs ->
    try
      let (node_id, vector_tag_id) = Keys.decode_vector_owner_bs owner_bs in
      let vector_tag = Store.unintern db ?txn vector_tag_id in
      Ok { vector_id; node_id; vector_tag }
    with 
    | Not_found | Lmdb.Not_found -> 
      Error (Corrupted_data "vector tag intern_id not found in reverse lookup")
    | Lmdb.Map_full -> Error Storage_full
    | Lmdb.Error code -> Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

(** delete a vector and clean up indexes *)
let delete_vector (db : t) ?txn (vector_id : vector_id) : (unit, error) result =
  let key = Keys.encode_id_bs vector_id in
  let owner_bs =
    try Some (Lmdb.Map.get db.vector_owners ?txn key)
    with Not_found | Lmdb.Not_found -> None
  in
  match owner_bs with
  | None -> Error (Vector_not_found vector_id)
  | Some owner_bs ->
    try
      let (node_id, vector_tag_id) = Keys.decode_vector_owner_bs owner_bs in
      Lmdb.Map.remove db.vectors ?txn key;
      Lmdb.Map.remove db.vector_owners ?txn key;
      let index_key = Keys.encode_vector_index_bs ~node_id ~vector_tag_id ~vector_id in
      Lmdb.Map.remove db.vector_index ?txn index_key;
      Ok ()
    with 
    | Not_found | Lmdb.Not_found -> 
      Error (Corrupted_data "vector index entry missing during delete")
    | Lmdb.Map_full -> Error Storage_full
    | Lmdb.Error code -> Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

(** get all vectors for a node, optionally filtered by tag *)
let get_vectors_for_node (db : t) ?txn (node_id : node_id) ?vector_tag () : (vector_info list, error) result =
  let vector_tag_id_opt = match vector_tag with
    | None -> None
    | Some tag -> Store.lookup_intern db ?txn tag
  in
  match vector_tag, vector_tag_id_opt with
  | Some _, None -> Ok []
  | _ ->
    let prefix = Keys.encode_vector_index_prefix_bs ~node_id ?vector_tag_id:vector_tag_id_opt () in
    let prefix_len = Bigstring.length prefix in
    let txn_ro = Option.map (fun t -> (t :> [`Read] Lmdb.Txn.t)) txn in
    try
      let results = Lmdb.Cursor.go Lmdb.Ro ?txn:txn_ro db.vector_index (fun cursor ->
        let rec collect acc =
          try
            let (key, _) = Lmdb.Cursor.next cursor in
            if Bigstring.length key >= prefix_len && Keys.bigstring_has_prefix ~prefix key then
              let (_, tag_id, vid) = Keys.decode_vector_index_bs key in
              collect ((vid, tag_id) :: acc)
            else
              acc
          with Lmdb.Not_found -> acc
        in
        try
          let (key, _) = Lmdb.Cursor.seek_range cursor prefix in
          if Bigstring.length key >= prefix_len && Keys.bigstring_has_prefix ~prefix key then
            let (_, tag_id, vid) = Keys.decode_vector_index_bs key in
            List.rev (collect [(vid, tag_id)])
          else
            []
        with Lmdb.Not_found -> []
      ) in
      let rec map_with_unintern acc = function
        | [] -> Ok (List.rev acc)
        | (vid, tag_id) :: rest ->
          try
            let vtag = Store.unintern db ?txn tag_id in
            map_with_unintern ({ vector_id = vid; node_id; vector_tag = vtag } :: acc) rest
          with Not_found | Lmdb.Not_found ->
            Error (Corrupted_data "vector tag intern_id not found in reverse lookup")
      in
      map_with_unintern [] results
    with
    | Lmdb.Map_full -> Error Storage_full
    | Lmdb.Error code -> Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

(** {1 k-NN search} *)

type distance_metric = Euclidean | Cosine | DotProduct

(** convert bigstring of float32s to float array *)
let bigstring_to_floats (bs : bigstring) : float array =
  let n = Bigstring.length bs / 4 in
  Array.init n (fun i ->
    Int32.float_of_bits (Bigstring.get_int32_le bs (i * 4)))

(** compute distance between two float arrays. returns infinity for dimension mismatch *)
let compute_distance (metric : distance_metric) (a : float array) (b : float array) : float =
  let n = Array.length a in
  if Array.length b <> n then infinity
  else match metric with
  | Euclidean ->
    let sum = ref 0.0 in
    for i = 0 to n - 1 do
      let d = a.(i) -. b.(i) in
      sum := !sum +. (d *. d)
    done;
    sqrt !sum
  | Cosine ->
    let dot = ref 0.0 in
    let norm_a = ref 0.0 in
    let norm_b = ref 0.0 in
    for i = 0 to n - 1 do
      dot := !dot +. (a.(i) *. b.(i));
      norm_a := !norm_a +. (a.(i) *. a.(i));
      norm_b := !norm_b +. (b.(i) *. b.(i))
    done;
    let denom = sqrt !norm_a *. sqrt !norm_b in
    if denom = 0.0 then 1.0  (* zero vector: max distance *)
    else 1.0 -. (!dot /. denom)
  | DotProduct ->
    let dot = ref 0.0 in
    for i = 0 to n - 1 do
      dot := !dot +. (a.(i) *. b.(i))
    done;
    -. !dot  (* negate so smaller = better match *)

type knn_result = {
  vector_id : vector_id;
  node_id : node_id;
  vector_tag : string;
  distance : float;
}

(** brute force k-NN search over all vectors *)
let knn_brute_force (db : t) ?txn ~(metric : distance_metric) ~(k : int) (query : float array) : (knn_result list, error) result =
  let txn_ro = Option.map (fun t -> (t :> [`Read] Lmdb.Txn.t)) txn in
  try
    let results = Lmdb.Cursor.go Lmdb.Ro ?txn:txn_ro db.vectors (fun cursor ->
      let rec collect acc =
        try
          let (key, value) = Lmdb.Cursor.next cursor in
          let vid = Keys.decode_id_bs key in
          let vec = bigstring_to_floats value in
          let dist = compute_distance metric query vec in
          collect ((vid, dist) :: acc)
        with Lmdb.Not_found -> acc
      in
      try
        let (key, value) = Lmdb.Cursor.first cursor in
        let vid = Keys.decode_id_bs key in
        let vec = bigstring_to_floats value in
        let dist = compute_distance metric query vec in
        collect [(vid, dist)]
      with Lmdb.Not_found -> []
    ) in
    let finite_results = List.filter (fun (_, d) -> Float.is_finite d) results in
    let sorted = List.sort (fun (_, d1) (_, d2) -> Float.compare d1 d2) finite_results in
    let top_k = List.filteri (fun i _ -> i < k) sorted in
    let enriched = List.filter_map (fun (vid, dist) ->
      match get_vector_info db ?txn vid with
      | Ok info -> Some { vector_id = vid; node_id = info.node_id; vector_tag = info.vector_tag; distance = dist }
      | Error _ -> None
    ) top_k in
    Ok enriched
  with
  | Not_found | Lmdb.Not_found -> Ok []
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code -> Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

(** brute force k-NN search with bigstring query (float32 array in memory) *)
let knn_brute_force_bs (db : t) ?txn ~(metric : distance_metric) ~(k : int) (query : bigstring) : (knn_result list, error) result =
  let query_floats = bigstring_to_floats query in
  knn_brute_force db ?txn ~metric ~k query_floats
