type bigstring = Types.bigstring
type id = Types.id
type intern_id = Types.intern_id
type node_id = Types.node_id
type edge_id = Types.edge_id
type vector_id = Types.vector_id
type vector_tag_id = Types.vector_tag_id
type owner_kind = Types.owner_kind = Node | Edge

module Owner = Types.Owner

type node_info = Types.node_info = { id : node_id; node_type : string }

type edge_info = Types.edge_info = {
  id : edge_id;
  edge_type : string;
  src : node_id;
  dst : node_id;
}

type vector_info = Types.vector_info = {
  vector_id : vector_id;
  owner_kind : owner_kind;
  owner_id : id;
  vector_tag : string;
}

type distance_metric = Types.distance_metric = Euclidean | Cosine | DotProduct

type knn_result = Types.knn_result = {
  vector_id : vector_id;
  owner_kind : owner_kind;
  owner_id : id;
  vector_tag : string;
  distance : float;
}

type error = Types.error =
  | Node_not_found of node_id
  | Edge_not_found of edge_id
  | Vector_not_found of vector_id
  | Storage_full
  | Storage_error of string
  | Corrupted_data of string

module Error = Types.Error

type 'perm txn = 'perm Types.txn constraint 'perm = [< `Read | `Write ]
type ro_txn = Types.ro_txn
type rw_txn = Types.rw_txn

let ( let* ) = Types.( let* )

module Bigstring = Bigstringaf
module Bigstring_message = Bigstring_message

type t = {
  db : Types.t;
  hnsw_mvcc : (string, Hnsw_mvcc.t) Hashtbl.t;
  db_path : string;
}

let with_transaction (t : t) (f : rw_txn -> 'a) : 'a option =
  Types.with_transaction t.db f

let with_transaction_ro (t : t) (f : ro_txn -> 'a) : 'a option =
  Types.with_transaction_ro t.db f

let abort_transaction : 'perm txn -> 'a = Types.abort_transaction

let fold_all map ?txn f init =
  let txn_ro = Option.map (fun t -> (t :> [ `Read ] Lmdb.Txn.t)) txn in
  try
    Lmdb.Cursor.go Lmdb.Ro ?txn:txn_ro map (fun cursor ->
        let rec scan acc =
          try
            let key, value = Lmdb.Cursor.next cursor in
            scan (f acc key value)
          with Lmdb.Not_found -> acc
        in
        try
          let key, value = Lmdb.Cursor.first cursor in
          scan (f init key value)
        with Lmdb.Not_found -> init)
  with Lmdb.Not_found -> init

let fold_prefix map ?txn prefix f init =
  let prefix_len = Bigstring.length prefix in
  let has_prefix key =
    Bigstring.length key >= prefix_len && Keys.bigstring_has_prefix ~prefix key
  in
  let txn_ro = Option.map (fun t -> (t :> [ `Read ] Lmdb.Txn.t)) txn in
  try
    Lmdb.Cursor.go Lmdb.Ro ?txn:txn_ro map (fun cursor ->
        let rec scan acc =
          try
            let key, value = Lmdb.Cursor.next cursor in
            if has_prefix key then scan (f acc key value) else acc
          with Lmdb.Not_found -> acc
        in
        try
          let key, value = Lmdb.Cursor.seek_range cursor prefix in
          if has_prefix key then scan (f init key value) else init
        with Lmdb.Not_found -> init)
  with Lmdb.Not_found -> init

let find_best_entry_point mvcc table ~exclude_slot =
  let best_ep = ref (-1) in
  let best_level = ref (-1) in
  for i = 0 to Hnsw_mvcc.table_node_count table - 1 do
    if i <> exclude_slot then
      match Hnsw_mvcc.read_node mvcc table ~slot_id:i with
      | Some n when (not n.deleted) && n.layer_count - 1 > !best_level ->
          best_ep := i;
          best_level := n.layer_count - 1
      | _ -> ()
  done;
  (!best_ep, !best_level)

let get_all_vector_tags (db : Types.t) : (string * int64) list =
  let tag_ids = Hashtbl.create 16 in
  fold_all db.vector_owners
    (fun () _key value ->
      let _, _, tag_id, _ = Keys.decode_vector_owner_bs value in
      Hashtbl.replace tag_ids tag_id ())
    ();
  Hashtbl.fold
    (fun tag_id () acc ->
      try
        let tag_name = Store.unintern db tag_id in
        (tag_name, tag_id) :: acc
      with Not_found | Lmdb.Not_found -> acc)
    tag_ids []

let hnsw_epoch_key tag_name = "hnsw_epoch:" ^ tag_name

let get_lmdb_hnsw_epoch (db : Types.t) ?txn tag_name =
  try
    let bs = Lmdb.Map.get db.metadata ?txn (hnsw_epoch_key tag_name) in
    Bigstringaf.get_int64_le bs 0
  with Not_found | Lmdb.Not_found -> 0L

let set_lmdb_hnsw_epoch (db : Types.t) ?txn tag_name epoch =
  let bs = Bigstringaf.create 8 in
  Bigstringaf.set_int64_le bs 0 epoch;
  Lmdb.Map.set db.metadata ?txn (hnsw_epoch_key tag_name) bs

let reconcile_hnsw t tag_name =
  match Hashtbl.find_opt t.hnsw_mvcc tag_name with
  | None -> ()
  | Some mvcc ->
      let tag_id_opt = Store.lookup_intern t.db tag_name in
      let table = Hnsw_mvcc.begin_read mvcc in
      Fun.protect
        ~finally:(fun () -> Hnsw_mvcc.end_read mvcc table)
        (fun () ->
          let node_count = Hnsw_mvcc.table_node_count table in
          if node_count > 0 then begin
            (* build slot_id set from LMDB hnsw_slots for this tag *)
            let lmdb_slots =
              match tag_id_opt with
              | None -> Hashtbl.create 0
              | Some tag_id ->
                  let prefix = Keys.encode_id_bs tag_id in
                  fold_prefix t.db.hnsw_slots prefix
                    (fun tbl _key value ->
                      Hashtbl.replace tbl
                        (Keys.decode_hnsw_slot_value value)
                        true;
                      tbl)
                    (Hashtbl.create (node_count * 2))
            in
            (* scan HNSW nodes and fix discrepancies *)
            let hnsw_txn = Hnsw_mvcc.begin_write mvcc in
            let changed = ref false in
            for slot_id = 0 to node_count - 1 do
              match Hnsw_mvcc.read_node mvcc table ~slot_id with
              | Some node
                when (not node.deleted) && not (Hashtbl.mem lmdb_slots slot_id)
                ->
                  Hnsw_mvcc.write_node mvcc hnsw_txn ~slot_id
                    { node with deleted = true };
                  changed := true
              | Some node when node.deleted && Hashtbl.mem lmdb_slots slot_id ->
                  Hnsw_mvcc.write_node mvcc hnsw_txn ~slot_id
                    { node with deleted = false };
                  changed := true
              | _ -> ()
            done;
            if !changed then begin
              let ep, level =
                find_best_entry_point mvcc table ~exclude_slot:(-1)
              in
              Hnsw_mvcc.set_entry_point hnsw_txn ~entry_point:ep
                ~max_level:level;
              (* commit result matters for crash consistency *)
              match Hnsw_mvcc.commit mvcc hnsw_txn with
              | Ok () -> ()
              | Error e ->
                  Printf.eprintf "reconcile_hnsw: commit failed: %s\n%!"
                    (Hnsw_mvcc.error_to_string e)
            end
            else Hnsw_mvcc.rollback mvcc hnsw_txn
          end)

let open_hnsw_mvcc_files t =
  let tags = get_all_vector_tags t.db in
  List.iter
    (fun (tag_name, _tag_id) ->
      let file_path = Store.hnsw_file_path t.db_path tag_name ^ ".mvcc" in
      match Hnsw_mvcc.open_existing file_path with
      | Ok mvcc ->
          Hashtbl.replace t.hnsw_mvcc tag_name mvcc;
          let hnsw_epoch = Hnsw_mvcc.get_epoch mvcc in
          let lmdb_epoch = get_lmdb_hnsw_epoch t.db tag_name in
          if hnsw_epoch <> lmdb_epoch then begin
            (* epoch mismatch crash between HNSW commit and LMDB commit.
             Reconcile HNSW to match LMDB (source of truth), then sync epochs *)
            reconcile_hnsw t tag_name;
            let current_epoch = Hnsw_mvcc.get_epoch mvcc in
            ignore
              (Types.with_transaction t.db (fun txn ->
                   set_lmdb_hnsw_epoch t.db ~txn tag_name current_epoch))
          end
      | Error _ ->
          () (* file doesn't exist yet, will be created on first insert *))
    tags

let create ?map_size path =
  let* db = Store.create ?map_size path in
  let hnsw_mvcc = Hashtbl.create 16 in
  let t = { db; hnsw_mvcc; db_path = path } in
  open_hnsw_mvcc_files t;
  Ok t

let close t =
  Hashtbl.iter
    (fun _ mvcc ->
      Hnsw_mvcc.sync mvcc;
      Hnsw_mvcc.close mvcc)
    t.hnsw_mvcc;
  Store.close t.db

let register_node_schema_capnp t ?txn type_name schema_id =
  Props_capnp.register_node_schema t.db ?txn type_name schema_id

let register_edge_schema_capnp t ?txn type_name schema_id =
  Props_capnp.register_edge_schema t.db ?txn type_name schema_id

let set_node_props_capnp t ?txn node_id type_name build_fn init_root to_message
    =
  Props_capnp.set_node_props_capnp t.db ?txn node_id type_name build_fn
    init_root to_message

let get_node_props_capnp t ?txn node_id of_message read_fn =
  Props_capnp.get_node_props_capnp t.db ?txn node_id of_message read_fn

let set_edge_props_capnp t ?txn edge_id type_name build_fn init_root to_message
    =
  Props_capnp.set_edge_props_capnp t.db ?txn edge_id type_name build_fn
    init_root to_message

let get_edge_props_capnp t ?txn edge_id of_message read_fn =
  Props_capnp.get_edge_props_capnp t.db ?txn edge_id of_message read_fn

let get_edge_info (t : t) ?txn (edge_id : edge_id) : (edge_info, error) result =
  let* intern_id, src, dst = Props_capnp.get_edge_meta t.db ?txn edge_id in
  try
    let edge_type = Store.unintern t.db ?txn intern_id in
    Ok { id = edge_id; edge_type; src; dst }
  with Not_found | Lmdb.Not_found ->
    Error (Corrupted_data "edge type intern_id not found in reverse lookup")

let get_node_info (t : t) ?txn (node_id : node_id) : (node_info, error) result =
  let* intern_id = Props_capnp.get_node_meta t.db ?txn node_id in
  try
    let node_type = Store.unintern t.db ?txn intern_id in
    Ok { id = node_id; node_type }
  with Not_found | Lmdb.Not_found ->
    Error (Corrupted_data "node type intern_id not found in reverse lookup")

let create_node (t : t) ?txn (node_type : string) : (node_id, error) result =
  let* intern_id = Store.intern t.db ?txn node_type in
  Types.wrap_lmdb_exn (fun () ->
      let node_id = Store.get_next_id t.db ?txn Types.Metadata.next_node_id in
      let key = Keys.encode_id_bs node_id in
      Lmdb.Map.set t.db.node_meta ?txn key (Keys.encode_id_bs intern_id);
      Lmdb.Map.set t.db.nodes ?txn key Store.empty_bigstring;
      node_id)

let node_exists (t : t) ?txn (node_id : node_id) : (bool, error) result =
  try
    let _ = Lmdb.Map.get t.db.node_meta ?txn (Keys.encode_id_bs node_id) in
    Ok true
  with
  | Not_found | Lmdb.Not_found -> Ok false
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

let create_edge (t : t) ?txn (edge_type : string) (src : node_id)
    (dst : node_id) : (edge_id, error) result =
  let* intern_id = Store.intern t.db ?txn edge_type in
  Types.wrap_lmdb_exn (fun () ->
      let edge_id = Store.get_next_id t.db ?txn Types.Metadata.next_edge_id in
      let key = Keys.encode_id_bs edge_id in
      Lmdb.Map.set t.db.edge_meta ?txn key
        (Keys.encode_edge_meta ~type_id:intern_id ~src ~dst);
      Lmdb.Map.set t.db.edges ?txn key Store.empty_bigstring;
      let outbound_key =
        Keys.encode_adjacency_bs ~node_id:src ~intern_id ~opposite_id:dst
          ~edge_id
      in
      let inbound_key =
        Keys.encode_adjacency_bs ~node_id:dst ~intern_id ~opposite_id:src
          ~edge_id
      in
      Lmdb.Map.set t.db.outbound ?txn outbound_key Store.empty_bigstring;
      Lmdb.Map.set t.db.inbound ?txn inbound_key Store.empty_bigstring;
      edge_id)

let edge_exists (t : t) ?txn (edge_id : edge_id) : (bool, error) result =
  try
    let _ = Lmdb.Map.get t.db.edge_meta ?txn (Keys.encode_id_bs edge_id) in
    Ok true
  with
  | Not_found | Lmdb.Not_found -> Ok false
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

let delete_edge_data (t : t) ?txn (edge_id : edge_id) : (unit, error) result =
  let key = Keys.encode_id_bs edge_id in
  try
    let meta = Lmdb.Map.get t.db.edge_meta ?txn key in
    let intern_id, src, dst = Keys.decode_edge_meta meta in
    Lmdb.Map.remove t.db.edges ?txn key;
    Lmdb.Map.remove t.db.edge_meta ?txn key;
    let outbound_key =
      Keys.encode_adjacency_bs ~node_id:src ~intern_id ~opposite_id:dst ~edge_id
    in
    let inbound_key =
      Keys.encode_adjacency_bs ~node_id:dst ~intern_id ~opposite_id:src ~edge_id
    in
    Lmdb.Map.remove t.db.outbound ?txn outbound_key;
    Lmdb.Map.remove t.db.inbound ?txn inbound_key;
    Ok ()
  with
  | Not_found | Lmdb.Not_found -> Error (Edge_not_found edge_id)
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))
  | Invalid_argument msg -> Error (Corrupted_data msg)

type direction = Outbound | Inbound

let scan_adjacency_index (t : t) ?txn ~direction ~node_id
    (map : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t) (prefix : bigstring) :
    (edge_info list, error) result =
  try
    let tuples =
      fold_prefix map ?txn prefix
        (fun acc key _value ->
          let _, intern_id, opposite_id, edge_id =
            Keys.decode_adjacency_bs key
          in
          (edge_id, intern_id, opposite_id) :: acc)
        []
      |> List.rev
    in
    Ok
      (List.map
         (fun (edge_id, intern_id, opposite_id) ->
           let edge_type = Store.unintern t.db ?txn intern_id in
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

let get_outbound_edges (t : t) ?txn (node_id : node_id) :
    (edge_info list, error) result =
  let prefix = Keys.encode_adjacency_prefix_bs ~node_id () in
  scan_adjacency_index t ?txn ~direction:Outbound ~node_id t.db.outbound prefix

let get_inbound_edges (t : t) ?txn (node_id : node_id) :
    (edge_info list, error) result =
  let prefix = Keys.encode_adjacency_prefix_bs ~node_id () in
  scan_adjacency_index t ?txn ~direction:Inbound ~node_id t.db.inbound prefix

let get_outbound_edges_by_type (t : t) ?txn (node_id : node_id)
    (edge_type : string) : (edge_info list, error) result =
  match Store.lookup_intern t.db ?txn edge_type with
  | None -> Ok [] (* type not interned means no edges of this type exist *)
  | Some intern_id ->
      let prefix = Keys.encode_adjacency_prefix_bs ~node_id ~intern_id () in
      scan_adjacency_index t ?txn ~direction:Outbound ~node_id t.db.outbound
        prefix

let get_inbound_edges_by_type (t : t) ?txn (node_id : node_id)
    (edge_type : string) : (edge_info list, error) result =
  match Store.lookup_intern t.db ?txn edge_type with
  | None -> Ok [] (* type not interned means no edges of this type exist *)
  | Some intern_id ->
      let prefix = Keys.encode_adjacency_prefix_bs ~node_id ~intern_id () in
      scan_adjacency_index t ?txn ~direction:Inbound ~node_id t.db.inbound
        prefix

external dist_from_mmap :
  Common.bigstring ->
  float array ->
  (int [@untagged]) ->
  (float [@unboxed]) ->
  (int [@untagged]) ->
  (int [@untagged]) ->
  (float [@unboxed]) =
  "gvecdb_dist_from_mmap_bc" "gvecdb_dist_from_mmap"
  [@@noalloc]

let make_compute_distance (vector_file : Vector_file.t) metric normalized_query
    query_norm _vec_arr dim =
  let mmap = vector_file.mmap in
  let metric_int = Types.metric_to_int metric in
  fun other_offset ->
    dist_from_mmap mmap normalized_query (Int64.to_int other_offset) query_norm
      metric_int dim

let compute_pairwise_distance vector_file metric offset_a offset_b =
  match
    ( Vector_file.read_vector_with_header vector_file offset_a,
      Vector_file.read_vector_with_header vector_file offset_b )
  with
  | Ok (bs_a, hdr_a), Ok (bs_b, hdr_b) ->
      let dim_a = hdr_a.Vector_file.dim in
      let dim_b = hdr_b.Vector_file.dim in
      if dim_a <> dim_b then infinity
      else
        let arr_a = Float32_vec.to_array bs_a in
        let normalized_a = Array.copy arr_a in
        let norm_a = Knn.normalize_array normalized_a in
        if Vector_file.is_normalized hdr_b then
          Knn.compute_distance_normalized metric normalized_a norm_a bs_b
            hdr_b.Vector_file.norm dim_a
        else
          let norm_sq_a = norm_a *. norm_a in
          Knn.compute_distance_raw metric arr_a norm_sq_a bs_b
            hdr_b.Vector_file.norm dim_a
  | _ -> infinity

let get_or_create_hnsw_mvcc t ?(metric = Cosine) vector_tag =
  match Hashtbl.find_opt t.hnsw_mvcc vector_tag with
  | Some f -> Some f
  | None -> (
      let file_path = Store.hnsw_file_path t.db_path vector_tag ^ ".mvcc" in
      match
        Hnsw_mvcc.create file_path ~metric ~params:Hnsw.default_params ()
      with
      | Error _ -> None
      | Ok f ->
          Hashtbl.replace t.hnsw_mvcc vector_tag f;
          Some f)

let create_vector_internal (t : t) ~txn ~normalize ~metric
    (owner_kind : owner_kind) (owner_id : id) (vector_tag : string)
    (data : bigstring) :
    (vector_id, error) result =
  let* vector_tag_id = Store.intern t.db ~txn vector_tag in
  let dim = Float32_vec.dim data in
  let store_data, norm =
    if normalize then Float32_vec.normalize data
    else (data, sqrt (Float32_vec.norm_sq data))
  in
  match Vector_file.allocate t.db.vector_file dim with
  | Error e -> Error (Storage_error (Vector_file.error_to_string e))
  | Ok file_offset -> (
      match
        Vector_file.write_vector_at t.db.vector_file file_offset
          ~normalized:normalize store_data norm
      with
      | Error e -> Error (Storage_error (Vector_file.error_to_string e))
      | Ok () -> (
          match get_or_create_hnsw_mvcc t vector_tag with
          | None -> Error (Storage_error "failed to create HNSW file")
          | Some mvcc -> (
              let vector_id_result =
                Types.wrap_lmdb_exn (fun () ->
                    Store.get_next_id t.db ~txn Types.Metadata.next_vector_id)
              in
              match vector_id_result with
              | Error e -> Error e
              | Ok vector_id -> (
                  let vec_arr = Float32_vec.to_array store_data in
                  let normalized_query = Array.copy vec_arr in
                  let query_norm = Knn.normalize_array normalized_query in
                  let metric = Hnsw_mvcc.get_metric mvcc in
                  let compute_distance =
                    make_compute_distance t.db.vector_file metric
                      normalized_query query_norm vec_arr dim
                  in
                  let pairwise_distance =
                    compute_pairwise_distance t.db.vector_file metric
                  in

                  let hnsw_txn = Hnsw_mvcc.begin_write mvcc in
                  match
                    Hnsw_mvcc.insert_mvcc mvcc hnsw_txn ~vector_id
                      ~vector_offset:file_offset ~compute_distance
                      ~compute_pairwise_distance:pairwise_distance
                      ~dimension:dim
                  with
                  | Error (Hnsw_mvcc.Corrupted_data msg) ->
                      Hnsw_mvcc.rollback mvcc hnsw_txn;
                      Error (Corrupted_data ("dimension mismatch: " ^ msg))
                  | Error e ->
                      Hnsw_mvcc.rollback mvcc hnsw_txn;
                      Error (Storage_error (Hnsw_mvcc.error_to_string e))
                  | Ok slot_id -> (
                      match Hnsw_mvcc.commit mvcc hnsw_txn with
                      | Error e ->
                          Error (Storage_error (Hnsw_mvcc.error_to_string e))
                      | Ok () ->
                          (* store HNSW epoch for crash consistency *)
                          set_lmdb_hnsw_epoch t.db ~txn vector_tag
                            (Hnsw_mvcc.get_epoch mvcc);
                          (* store in LMDB *)
                          Types.wrap_lmdb_exn (fun () ->
                              let key = Keys.encode_id_bs vector_id in
                              let owner_value =
                                Keys.encode_vector_owner_bs ~owner_kind
                                  ~owner_id ~vector_tag_id ~file_offset
                              in
                              Lmdb.Map.set t.db.vector_owners ~txn key
                                owner_value;
                              let index_key =
                                Keys.encode_vector_index_bs ~owner_kind
                                  ~owner_id ~vector_tag_id ~vector_id
                              in
                              Lmdb.Map.set t.db.vector_index ~txn index_key
                                Store.empty_bigstring;
                              (* store slot mapping *)
                              let slot_key =
                                Keys.encode_hnsw_slot_key ~tag_id:vector_tag_id
                                  ~vector_id
                              in
                              Lmdb.Map.set t.db.hnsw_slots ~txn slot_key
                                (Keys.encode_hnsw_slot_value slot_id);
                              vector_id))))))

let create_vector (t : t) ~txn ?(normalize = true) ?(metric = Cosine)
    (node_id : node_id) (vector_tag : string) (data : bigstring) :
    (vector_id, error) result =
  let* exists = node_exists t ~txn node_id in
  if not exists then Error (Node_not_found node_id)
  else
    create_vector_internal t ~txn ~normalize ~metric Node node_id vector_tag
      data

let create_edge_vector (t : t) ~txn ?(normalize = true) ?(metric = Cosine)
    (edge_id : edge_id) (vector_tag : string) (data : bigstring) :
    (vector_id, error) result =
  let* exists = edge_exists t ~txn edge_id in
  if not exists then Error (Edge_not_found edge_id)
  else
    create_vector_internal t ~txn ~normalize ~metric Edge edge_id vector_tag
      data

let vector_exists (t : t) ?txn (vector_id : vector_id) : (bool, error) result =
  try
    let _ =
      Lmdb.Map.get t.db.vector_owners ?txn (Keys.encode_id_bs vector_id)
    in
    Ok true
  with
  | Not_found | Lmdb.Not_found -> Ok false
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

let get_vector (t : t) ?txn (vector_id : vector_id) : (bigstring, error) result
    =
  let key = Keys.encode_id_bs vector_id in
  try
    let owner_bs = Lmdb.Map.get t.db.vector_owners ?txn key in
    let _, _, _, file_offset = Keys.decode_vector_owner_bs owner_bs in
    match Vector_file.read_vector_at t.db.vector_file file_offset with
    | Ok data -> Ok data
    | Error e -> Error (Storage_error (Vector_file.error_to_string e))
  with
  | Not_found | Lmdb.Not_found -> Error (Vector_not_found vector_id)
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

let get_vector_info (t : t) ?txn (vector_id : vector_id) :
    (vector_info, error) result =
  let key = Keys.encode_id_bs vector_id in
  let owner_bs =
    try Some (Lmdb.Map.get t.db.vector_owners ?txn key)
    with Not_found | Lmdb.Not_found -> None
  in
  match owner_bs with
  | None -> Error (Vector_not_found vector_id)
  | Some owner_bs -> (
      try
        let owner_kind, owner_id, vector_tag_id, _file_offset =
          Keys.decode_vector_owner_bs owner_bs
        in
        let vector_tag = Store.unintern t.db ?txn vector_tag_id in
        Ok { vector_id; owner_kind; owner_id; vector_tag }
      with
      | Not_found | Lmdb.Not_found ->
          Error
            (Corrupted_data "vector tag intern_id not found in reverse lookup")
      | Lmdb.Map_full -> Error Storage_full
      | Lmdb.Error code ->
          Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code)))

let delete_vector_internal (t : t) ?txn (vector_id : vector_id) :
    (unit, error) result =
  let key = Keys.encode_id_bs vector_id in
  let owner_bs =
    try Some (Lmdb.Map.get t.db.vector_owners ?txn key)
    with Not_found | Lmdb.Not_found -> None
  in
  match owner_bs with
  | None -> Error (Vector_not_found vector_id)
  | Some owner_bs -> (
      try
        let owner_kind, owner_id, vector_tag_id, _file_offset =
          Keys.decode_vector_owner_bs owner_bs
        in
        let vector_tag = Store.unintern t.db ?txn vector_tag_id in
        let slot_key =
          Keys.encode_hnsw_slot_key ~tag_id:vector_tag_id ~vector_id
        in
        let slot_id_opt =
          try
            Some
              (Keys.decode_hnsw_slot_value
                 (Lmdb.Map.get t.db.hnsw_slots ?txn slot_key))
          with Not_found | Lmdb.Not_found | Invalid_argument _ -> None
        in
        (* mark deleted in MVCC file *)
        (match (slot_id_opt, Hashtbl.find_opt t.hnsw_mvcc vector_tag) with
        | Some slot_id, Some mvcc ->
            let table = Hnsw_mvcc.begin_read mvcc in
            Fun.protect
              ~finally:(fun () -> Hnsw_mvcc.end_read mvcc table)
              (fun () ->
                match Hnsw_mvcc.read_node mvcc table ~slot_id with
                | Some node -> (
                    let deleted_node : Hnsw_page.node_data =
                      { node with deleted = true }
                    in
                    let hnsw_txn = Hnsw_mvcc.begin_write mvcc in
                    Hnsw_mvcc.write_node mvcc hnsw_txn ~slot_id deleted_node;
                    if slot_id = Hnsw_mvcc.table_entry_point table then begin
                      let ep, level =
                        find_best_entry_point mvcc table ~exclude_slot:slot_id
                      in
                      Hnsw_mvcc.set_entry_point hnsw_txn ~entry_point:ep
                        ~max_level:level
                    end;
                    match Hnsw_mvcc.commit mvcc hnsw_txn with
                    | Ok () ->
                        set_lmdb_hnsw_epoch t.db ?txn vector_tag
                          (Hnsw_mvcc.get_epoch mvcc)
                    | Error _ -> Hnsw_mvcc.rollback mvcc hnsw_txn)
                | None -> ())
        | _ -> ());
        (* remove slot mapping from LMDB *)
        (try Lmdb.Map.remove t.db.hnsw_slots ?txn slot_key
         with Not_found | Lmdb.Not_found -> ());
        (* remove from LMDB *)
        Lmdb.Map.remove t.db.vector_owners ?txn key;
        let index_key =
          Keys.encode_vector_index_bs ~owner_kind ~owner_id ~vector_tag_id
            ~vector_id
        in
        Lmdb.Map.remove t.db.vector_index ?txn index_key;
        Ok ()
      with
      | Not_found | Lmdb.Not_found ->
          Error (Corrupted_data "vector index entry missing during delete")
      | Lmdb.Map_full -> Error Storage_full
      | Lmdb.Error code ->
          Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code)))

let delete_vector (t : t) ~txn (vector_id : vector_id) : (unit, error) result =
  delete_vector_internal t ~txn vector_id

(* delete all vectors attached to an owner (node or edge). Used for cascade
   deletes. silently ignores already-deleted vectors *)
let delete_vectors_for_owner (t : t) ?txn (owner_kind : owner_kind)
    (owner_id : id) : (unit, error) result =
  let prefix = Keys.encode_vector_index_prefix_bs ~owner_kind ~owner_id () in
  try
    let vector_ids =
      fold_prefix t.db.vector_index ?txn prefix
        (fun acc key _value ->
          let _, _, _, vid = Keys.decode_vector_index_bs key in
          vid :: acc)
        []
    in
    let rec delete_all = function
      | [] -> Ok ()
      | vid :: rest -> (
          match delete_vector_internal t ?txn vid with
          | Ok () -> delete_all rest
          | Error (Vector_not_found _) -> delete_all rest
          | Error e -> Error e)
    in
    delete_all vector_ids
  with
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

let delete_edge (t : t) ?txn (edge_id : edge_id) : (unit, error) result =
  let* () = delete_vectors_for_owner t ?txn Edge edge_id in
  delete_edge_data t ?txn edge_id

let delete_node (t : t) ?txn (node_id : node_id) : (unit, error) result =
  let key = Keys.encode_id_bs node_id in
  (* check node exists first *)
  let node_meta_exists =
    try
      let _ = Lmdb.Map.get t.db.node_meta ?txn key in
      true
    with Not_found | Lmdb.Not_found -> false
  in
  if not node_meta_exists then Error (Node_not_found node_id)
  else
    try
      (* 1. delete all vectors directly attached to this node *)
      let* () = delete_vectors_for_owner t ?txn Node node_id in
      (* 2. get all outbound edges and delete them (with their vectors) *)
      let* outbound_edges = get_outbound_edges t ?txn node_id in
      let rec delete_edges = function
        | [] -> Ok ()
        | edge :: rest ->
            let* () = delete_vectors_for_owner t ?txn Edge edge.id in
            let* () = delete_edge_data t ?txn edge.id in
            delete_edges rest
      in
      let* () = delete_edges outbound_edges in
      (* 3. get all inbound edges and delete them (with their vectors) *)
      let* inbound_edges = get_inbound_edges t ?txn node_id in
      let* () = delete_edges inbound_edges in
      (* 4. delete the node itself *)
      Lmdb.Map.remove t.db.nodes ?txn key;
      Lmdb.Map.remove t.db.node_meta ?txn key;
      Ok ()
    with
    | Lmdb.Map_full -> Error Storage_full
    | Lmdb.Error code ->
        Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

let get_vectors_for_owner_internal (t : t) ?txn (owner_kind : owner_kind)
    (owner_id : id) ?vector_tag () : (vector_info list, error) result =
  let vector_tag_id_opt =
    match vector_tag with
    | None -> None
    | Some tag -> Store.lookup_intern t.db ?txn tag
  in
  match (vector_tag, vector_tag_id_opt) with
  | Some _, None -> Ok []
  | _ -> (
      let prefix =
        Keys.encode_vector_index_prefix_bs ~owner_kind ~owner_id
          ?vector_tag_id:vector_tag_id_opt ()
      in
      try
        let results =
          fold_prefix t.db.vector_index ?txn prefix
            (fun acc key _value ->
              let _, _, tag_id, vid = Keys.decode_vector_index_bs key in
              (vid, tag_id) :: acc)
            []
          |> List.rev
        in
        let rec map_with_unintern acc = function
          | [] -> Ok (List.rev acc)
          | (vid, tag_id) :: rest -> (
              try
                let vtag = Store.unintern t.db ?txn tag_id in
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

let get_vectors_for_node (t : t) ?txn (node_id : node_id) ?vector_tag () :
    (vector_info list, error) result =
  get_vectors_for_owner_internal t ?txn Node node_id ?vector_tag ()

let get_vectors_for_edge (t : t) ?txn (edge_id : edge_id) ?vector_tag () :
    (vector_info list, error) result =
  get_vectors_for_owner_internal t ?txn Edge edge_id ?vector_tag ()

let knn_brute_force t ?txn ~metric ~k query =
  Knn.brute_force t.db ?txn ~metric ~k query

let knn_brute_force_bs t ?txn ~metric ~k query =
  Knn.brute_force_bs t.db ?txn ~metric ~k query

let metric_to_string = function
  | Types.Euclidean -> "euclidean"
  | Types.Cosine -> "cosine"
  | Types.DotProduct -> "dot_product"

let knn_hnsw (t : t) ?txn ~metric ~k ~ef ~vector_tag query =
  match Hashtbl.find_opt t.hnsw_mvcc vector_tag with
  | None -> Ok []
  | Some mvcc ->
      let index_metric = Hnsw_mvcc.get_metric mvcc in
      if metric <> index_metric then
        Error
          (Storage_error
             (Printf.sprintf "metric mismatch: requested %s but index uses %s"
                (metric_to_string metric)
                (metric_to_string index_metric)))
      else
        (* capture MVCC snapshot *)
        let table = Hnsw_mvcc.begin_read mvcc in
        Fun.protect
          ~finally:(fun () -> Hnsw_mvcc.end_read mvcc table)
          (fun () ->
            if Hnsw_mvcc.table_entry_point table < 0 then Ok []
            else begin
              let dim = Array.length query in
              let index_dim = Hnsw_mvcc.get_dimension mvcc in
              if index_dim > 0 && dim <> index_dim then
                Error
                  (Storage_error
                     (Printf.sprintf
                        "query dimension mismatch: index has %d, query has %d"
                        index_dim dim))
              else begin
                let normalized_query = Array.copy query in
                let query_norm = Knn.normalize_array normalized_query in
                let dist_from_offset =
                  make_compute_distance t.db.vector_file metric normalized_query
                    query_norm query dim
                in

                let ctx =
                  Hnsw_mvcc.create_search_context mvcc table ~dist_from_offset
                    ~overlay:None
                in
                let results = Hnsw_mvcc.search_mvcc ctx ~k ~ef in

                (* convert to knn_result *)
                let results_with_info =
                  List.filter_map
                    (fun (slot_id, dist) ->
                      match Hnsw_mvcc.read_node mvcc table ~slot_id with
                      | None -> None
                      | Some node -> (
                          match get_vector_info t ?txn node.vector_id with
                          | Ok info ->
                              Some
                                {
                                  vector_id = node.vector_id;
                                  owner_kind = info.owner_kind;
                                  owner_id = info.owner_id;
                                  vector_tag = info.vector_tag;
                                  distance = dist;
                                }
                          | Error _ -> None))
                    results
                in
                Ok results_with_info
              end
            end)

let knn_hnsw_bs t ?txn ~metric ~k ~ef ~vector_tag query =
  knn_hnsw t ?txn ~metric ~k ~ef ~vector_tag (Float32_vec.to_array query)

let rebuild_hnsw_index (t : t) ?(txn : rw_txn option) ~vector_tag () =
  let tag_id_opt = Store.lookup_intern t.db ?txn vector_tag in
  let get_all_vectors () =
    match tag_id_opt with
    | None -> []
    | Some target_tag_id ->
        fold_all t.db.vector_owners ?txn
          (fun acc key value ->
            let vid = Keys.decode_id_bs key in
            let _owner_kind, _owner_id, vtag_id, offset =
              Keys.decode_vector_owner_bs value
            in
            if vtag_id = target_tag_id then (vid, offset) :: acc else acc)
          []
  in
  let clear_slot_mappings tag_id =
    let prefix = Keys.encode_id_bs tag_id in
    let prefix_len = Bigstring.length prefix in
    let has_prefix key =
      Bigstring.length key >= prefix_len
      && Keys.bigstring_has_prefix ~prefix key
    in
    try
      Lmdb.Cursor.go Lmdb.Rw ?txn t.db.hnsw_slots (fun cursor ->
          let rec delete_matching () =
            try
              let key, _ = Lmdb.Cursor.next cursor in
              if has_prefix key then begin
                Lmdb.Cursor.remove cursor;
                delete_matching ()
              end
            with Lmdb.Not_found -> ()
          in
          try
            let key, _ = Lmdb.Cursor.seek_range cursor prefix in
            if has_prefix key then begin
              Lmdb.Cursor.remove cursor;
              delete_matching ()
            end
          with Lmdb.Not_found -> ())
    with Not_found | Lmdb.Not_found | Lmdb.Error _ -> ()
  in
  let metric =
    match Hashtbl.find_opt t.hnsw_mvcc vector_tag with
    | Some mvcc -> Hnsw_mvcc.get_metric mvcc
    | None -> Cosine
  in
  let file_path = Store.hnsw_file_path t.db_path vector_tag ^ ".mvcc" in
  (match Hashtbl.find_opt t.hnsw_mvcc vector_tag with
  | Some old_file ->
      Hnsw_mvcc.close old_file;
      Hashtbl.remove t.hnsw_mvcc vector_tag
  | None -> ());
  (match tag_id_opt with
  | Some tag_id -> clear_slot_mappings tag_id
  | None -> ());
  match Hnsw_mvcc.create file_path ~metric ~params:Hnsw.default_params with
  | Error e -> Error (Storage_error (Hnsw_mvcc.error_to_string e))
  | Ok mvcc ->
      let vectors = get_all_vectors () in
      if vectors = [] then begin
        Hashtbl.replace t.hnsw_mvcc vector_tag mvcc;
        Ok ()
      end
      else begin
        (* get dimension from first vector *)
        let dim =
          match List.hd vectors with
          | _, offset -> (
              match
                Vector_file.read_vector_with_header t.db.vector_file offset
              with
              | Ok (bs, _) -> Float32_vec.dim bs
              | Error _ -> 0)
        in
        if dim = 0 then begin
          Hashtbl.replace t.hnsw_mvcc vector_tag mvcc;
          Ok ()
        end
        else begin
          let hnsw_txn = Hnsw_mvcc.begin_write mvcc in
          let result =
            List.fold_left
              (fun acc (vector_id, vector_offset) ->
                match acc with
                | Error _ -> acc
                | Ok () -> (
                    match
                      Vector_file.read_vector_with_header t.db.vector_file
                        vector_offset
                    with
                    | Error _ -> acc
                    | Ok (vec_bs, _hdr) -> (
                        let vec_arr = Float32_vec.to_array vec_bs in
                        let normalized_query = Array.copy vec_arr in
                        let query_norm = Knn.normalize_array normalized_query in
                        let compute_distance =
                          make_compute_distance t.db.vector_file metric
                            normalized_query query_norm vec_arr dim
                        in
                        let pairwise_distance =
                          compute_pairwise_distance t.db.vector_file metric
                        in
                        match
                          Hnsw_mvcc.insert_mvcc mvcc hnsw_txn ~vector_id
                            ~vector_offset ~compute_distance
                            ~compute_pairwise_distance:pairwise_distance
                            ~dimension:dim
                        with
                        | Error (Hnsw_mvcc.Corrupted_data msg) ->
                            Error
                              (Corrupted_data ("dimension mismatch: " ^ msg))
                        | Error e ->
                            Error (Storage_error (Hnsw_mvcc.error_to_string e))
                        | Ok slot_id ->
                            (* Store slot mapping in LMDB *)
                            (match tag_id_opt with
                            | Some tag_id ->
                                let slot_key =
                                  Keys.encode_hnsw_slot_key ~tag_id ~vector_id
                                in
                                Lmdb.Map.set t.db.hnsw_slots ?txn slot_key
                                  (Keys.encode_hnsw_slot_value slot_id)
                            | None -> ());
                            Ok ())))
              (Ok ()) vectors
          in
          match result with
          | Error _ ->
              Hnsw_mvcc.rollback mvcc hnsw_txn;
              result
          | Ok () -> (
              match Hnsw_mvcc.commit mvcc hnsw_txn with
              | Error e -> Error (Storage_error (Hnsw_mvcc.error_to_string e))
              | Ok () ->
                  (* store HNSW epoch for crash consistency *)
                  set_lmdb_hnsw_epoch t.db ?txn vector_tag
                    (Hnsw_mvcc.get_epoch mvcc);
                  Hashtbl.replace t.hnsw_mvcc vector_tag mvcc;
                  Ok ())
        end
      end

module Types = Types
module Hnsw_page = Hnsw_page
module Hnsw_mvcc = Hnsw_mvcc
module Hnsw = Hnsw
