type bigstring = Types.bigstring
type id = Types.id
type intern_id = Types.intern_id
type node_id = Types.node_id
type edge_id = Types.edge_id
type vector_id = Types.vector_id
type vector_tag_id = Types.vector_tag_id
type owner_kind = Types.owner_kind = Node | Edge
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
          if node_count > 0 then (
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
            let hnsw_txn = Hnsw_mvcc.begin_write mvcc in
            let changed = ref false in
            let n_orphan = ref 0 in
            let n_restore = ref 0 in
            for slot_id = 0 to node_count - 1 do
              match Hnsw_mvcc.read_node_with_vec mvcc table ~slot_id with
              | Some node
                when (not node.deleted) && not (Hashtbl.mem lmdb_slots slot_id)
                ->
                  Hnsw_mvcc.write_node mvcc hnsw_txn ~slot_id
                    { node with deleted = true };
                  changed := true;
                  incr n_orphan
              | Some node when node.deleted && Hashtbl.mem lmdb_slots slot_id ->
                  Hnsw_mvcc.write_node mvcc hnsw_txn ~slot_id
                    { node with deleted = false };
                  changed := true;
                  incr n_restore
              | _ -> ()
            done;
            if !changed then (
              Printf.eprintf
                "gvecdb: reconcile tag=%s: %d orphans soft-deleted, %d \
                 restored (hnsw_nodes=%d lmdb_slots=%d)\n\
                 %!"
                tag_name !n_orphan !n_restore node_count
                (Hashtbl.length lmdb_slots);
              (* find entry point among non-orphan nodes only *)
              let best_ep = ref (-1) in
              let best_level = ref (-1) in
              for i = 0 to node_count - 1 do
                if Hashtbl.mem lmdb_slots i then
                  match Hnsw_mvcc.read_node mvcc table ~slot_id:i with
                  | Some n when n.layer_count - 1 > !best_level ->
                      best_ep := i;
                      best_level := n.layer_count - 1
                  | _ -> ()
              done;
              let ep = !best_ep in
              let level = !best_level in
              Hnsw_mvcc.set_entry_point hnsw_txn ~entry_point:ep
                ~max_level:level;
              match Hnsw_mvcc.commit mvcc hnsw_txn with
              | Ok () -> ()
              | Error e ->
                  Printf.eprintf "reconcile_hnsw: commit failed: %s\n%!"
                    (Hnsw_mvcc.error_to_string e))
            else Hnsw_mvcc.rollback mvcc hnsw_txn))

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
          if hnsw_epoch <> lmdb_epoch then (
            Printf.eprintf
              "gvecdb: epoch mismatch for tag=%s (hnsw=%Ld lmdb=%Ld), \
               reconciling...\n\
               %!"
              tag_name hnsw_epoch lmdb_epoch;
            reconcile_hnsw t tag_name;
            let current_epoch = Hnsw_mvcc.get_epoch mvcc in
            ignore
              (Types.with_transaction t.db (fun txn ->
                   set_lmdb_hnsw_epoch t.db ~txn tag_name current_epoch)))
      | Error _ -> ())
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

let filter_edges_with_predicates (t : t) ?txn (edges : edge_info list)
    (filters : Filter.filter_predicate list) : (edge_info list, error) result =
  if filters = [] then Ok edges
  else
    let pf_cache = Hashtbl.create 8 in
    let get_prepared et =
      match Hashtbl.find_opt pf_cache et with
      | Some cached -> cached
      | None ->
          let result =
            match Schema_registry.get_schema t.db ?txn et with
            | Ok schema -> (
                match Filter.prepare_filter schema filters with
                | Ok pf -> Some pf
                | Error _ -> None)
            | Error _ -> None
          in
          Hashtbl.replace pf_cache et result;
          result
    in
    Ok
      (List.filter
         (fun (ei : edge_info) ->
           match get_prepared ei.edge_type with
           | None -> false
           | Some pf -> (
               let key = Keys.encode_id_bs ei.id in
               try
                 let props_bs = Lmdb.Map.get t.db.edges ?txn key in
                 if Bigstring.length props_bs = 0 then false
                 else Filter.matches_blob props_bs pf
               with Not_found | Lmdb.Not_found -> false))
         edges)

let get_adjacency_edges_internal (t : t) ?txn ~direction ~node_id
    (map : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t) ?edge_type () :
    (edge_info list, error) result =
  match edge_type with
  | Some et -> (
      match Store.lookup_intern t.db ?txn et with
      | None -> Ok []
      | Some intern_id ->
          let prefix = Keys.encode_adjacency_prefix_bs ~node_id ~intern_id () in
          scan_adjacency_index t ?txn ~direction ~node_id map prefix)
  | None ->
      let prefix = Keys.encode_adjacency_prefix_bs ~node_id () in
      scan_adjacency_index t ?txn ~direction ~node_id map prefix

let get_outbound_edges (t : t) ?txn (node_id : node_id) ?edge_type ?filters () :
    (edge_info list, error) result =
  let* edges =
    get_adjacency_edges_internal t ?txn ~direction:Outbound ~node_id
      t.db.outbound ?edge_type ()
  in
  match filters with
  | Some f when f <> [] -> filter_edges_with_predicates t ?txn edges f
  | _ -> Ok edges

let get_inbound_edges (t : t) ?txn (node_id : node_id) ?edge_type ?filters () :
    (edge_info list, error) result =
  let* edges =
    get_adjacency_edges_internal t ?txn ~direction:Inbound ~node_id t.db.inbound
      ?edge_type ()
  in
  match filters with
  | Some f when f <> [] -> filter_edges_with_predicates t ?txn edges f
  | _ -> Ok edges

let make_compute_distance_hnsw mmap metric query_f32 query_norm dim =
  let metric_int = Types.metric_to_int metric in
  fun vec_off ->
    Float32_vec.dist_from_header mmap ~vec_off query_f32 ~query_norm
      ~metric:metric_int ~dim

let make_dist_from_inline metric query_f32 query_norm dim =
  let metric_int = Types.metric_to_int metric in
  fun (iv : bigstring) ->
    Float32_vec.dist_from_header iv ~vec_off:0 query_f32 ~query_norm
      ~metric:metric_int ~dim

let build_inline_vec ~normalized (store_data : bigstring) (norm : float) =
  let dim = Bigstringaf.length store_data / 4 in
  let total = Vector_file.vec_header_size + (dim * 4) in
  let bs = Bigstringaf.create total in
  Bigstringaf.set_int32_le bs 0 (Int32.of_int dim);
  let flags = if normalized then 0x01 else 0x00 in
  Bigstringaf.set bs 4 (Char.chr flags);
  for i = 0 to 2 do
    Bigstringaf.set bs (5 + i) '\x00'
  done;
  Bigstringaf.set_int64_le bs 8 (Int64.bits_of_float norm);
  Bigstringaf.blit store_data ~src_off:0 bs ~dst_off:Vector_file.vec_header_size
    ~len:(dim * 4);
  bs

let make_pairwise_distance_inline metric dim =
  let metric_int = Types.metric_to_int metric in
  let vec_data_off = Vector_file.vec_header_size in
  fun (buf_a : bigstring) (off_a : int) (buf_b : bigstring) (off_b : int) ->
    let data_len = dim * 4 in
    let query_f32 =
      Bigstringaf.sub buf_a ~off:(off_a + vec_data_off) ~len:data_len
    in
    let query_norm =
      Int64.float_of_bits (Bigstringaf.get_int64_le buf_a (off_a + 8))
    in
    Float32_vec.dist_from_header buf_b ~vec_off:off_b query_f32 ~query_norm
      ~metric:metric_int ~dim

let get_or_create_hnsw_mvcc t ?(metric = Cosine)
    ?(hnsw_params = Hnsw.default_params) vector_tag =
  match Hashtbl.find_opt t.hnsw_mvcc vector_tag with
  | Some f -> Some f
  | None -> (
      let file_path = Store.hnsw_file_path t.db_path vector_tag ^ ".mvcc" in
      match Hnsw_mvcc.create file_path ~metric ~params:hnsw_params () with
      | Error _ -> None
      | Ok f ->
          Hashtbl.replace t.hnsw_mvcc vector_tag f;
          Some f)

let create_vector_internal (t : t) ~txn ~normalize ~metric ?hnsw_params
    (owner_kind : owner_kind) (owner_id : id) (vector_tag : string)
    (data : bigstring) : (vector_id, error) result =
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
          match get_or_create_hnsw_mvcc t ~metric ?hnsw_params vector_tag with
          | None -> Error (Storage_error "failed to create HNSW file")
          | Some mvcc -> (
              let vector_id_result =
                Types.wrap_lmdb_exn (fun () ->
                    Store.get_next_id t.db ~txn Types.Metadata.next_vector_id)
              in
              match vector_id_result with
              | Error e -> Error e
              | Ok vector_id -> (
                  (* distance functions expect a normalised f32 vector and
                     the original pre-normalisation magnitude *)
                  let query_f32, query_norm =
                    if normalize then (store_data, norm)
                    else
                      let nv, n = Float32_vec.normalize store_data in
                      (nv, n)
                  in
                  let metric = Hnsw_mvcc.get_metric mvcc in
                  let hnsw_mmap = Hnsw_mvcc.get_mmap mvcc in
                  let compute_distance =
                    make_compute_distance_hnsw hnsw_mmap metric query_f32
                      query_norm dim
                  in
                  let dist_from_inline =
                    make_dist_from_inline metric query_f32 query_norm dim
                  in
                  let pairwise_distance =
                    make_pairwise_distance_inline metric dim
                  in
                  let inline_vec =
                    build_inline_vec ~normalized:normalize store_data norm
                  in

                  let hnsw_txn = Hnsw_mvcc.begin_write mvcc in
                  match
                    Hnsw_mvcc.insert_mvcc mvcc hnsw_txn ~vector_id ~inline_vec
                      ~compute_distance ~dist_from_inline
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

let create_vector_no_index (t : t) ~txn ~normalize ~metric
    (owner_kind : owner_kind) (owner_id : id) (vector_tag : string)
    (data : bigstring) : (vector_id, error) result =
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
          let _ = get_or_create_hnsw_mvcc t ~metric vector_tag in
          let vector_id_result =
            Types.wrap_lmdb_exn (fun () ->
                Store.get_next_id t.db ~txn Types.Metadata.next_vector_id)
          in
          match vector_id_result with
          | Error e -> Error e
          | Ok vector_id ->
              Types.wrap_lmdb_exn (fun () ->
                  let key = Keys.encode_id_bs vector_id in
                  let owner_value =
                    Keys.encode_vector_owner_bs ~owner_kind ~owner_id
                      ~vector_tag_id ~file_offset
                  in
                  Lmdb.Map.set t.db.vector_owners ~txn key owner_value;
                  let index_key =
                    Keys.encode_vector_index_bs ~owner_kind ~owner_id
                      ~vector_tag_id ~vector_id
                  in
                  Lmdb.Map.set t.db.vector_index ~txn index_key
                    Store.empty_bigstring;
                  vector_id)))

type batch_vector_request = {
  owner_kind : owner_kind;
  owner_id : id;
  vector_tag : string;
  data : bigstring;
  normalize : bool;
  metric : distance_metric;
}

let create_vectors_batch (t : t) ~txn (requests : batch_vector_request list) :
    (vector_id list, error) result =
  match requests with
  | [] -> Ok []
  | first :: _ -> (
      let vector_tag = first.vector_tag in
      let normalize = first.normalize in
      let metric = first.metric in
      match get_or_create_hnsw_mvcc t ~metric vector_tag with
      | None -> Error (Storage_error "failed to create HNSW file")
      | Some mvcc -> (
          let* vector_tag_id = Store.intern t.db ~txn vector_tag in
          let hnsw_txn = Hnsw_mvcc.begin_write mvcc in
          let dim = Float32_vec.dim first.data in
          let pairwise_distance =
            make_pairwise_distance_inline metric dim
          in
          let rec insert_all acc = function
            | [] -> Ok (List.rev acc)
            | req :: rest -> (
                let store_data, norm =
                  if normalize then Float32_vec.normalize req.data
                  else (req.data, sqrt (Float32_vec.norm_sq req.data))
                in
                match Vector_file.allocate t.db.vector_file dim with
                | Error e ->
                    Hnsw_mvcc.rollback mvcc hnsw_txn;
                    Error (Storage_error (Vector_file.error_to_string e))
                | Ok file_offset -> (
                    match
                      Vector_file.write_vector_at t.db.vector_file file_offset
                        ~normalized:normalize store_data norm
                    with
                    | Error e ->
                        Hnsw_mvcc.rollback mvcc hnsw_txn;
                        Error (Storage_error (Vector_file.error_to_string e))
                    | Ok () -> (
                        let vector_id_result =
                          Types.wrap_lmdb_exn (fun () ->
                              Store.get_next_id t.db ~txn
                                Types.Metadata.next_vector_id)
                        in
                        match vector_id_result with
                        | Error e ->
                            Hnsw_mvcc.rollback mvcc hnsw_txn;
                            Error e
                        | Ok vector_id -> (
                            let query_f32, query_norm =
                              if normalize then (store_data, norm)
                              else
                                let nv, n = Float32_vec.normalize store_data in
                                (nv, n)
                            in
                            let hnsw_metric = Hnsw_mvcc.get_metric mvcc in
                            let hnsw_mmap = Hnsw_mvcc.get_mmap mvcc in
                            let compute_distance =
                              make_compute_distance_hnsw hnsw_mmap hnsw_metric
                                query_f32 query_norm dim
                            in
                            let dist_from_inline =
                              make_dist_from_inline hnsw_metric query_f32
                                query_norm dim
                            in
                            let inline_vec =
                              build_inline_vec ~normalized:normalize store_data
                                norm
                            in
                            match
                              Hnsw_mvcc.insert_mvcc mvcc hnsw_txn ~vector_id
                                ~inline_vec ~compute_distance ~dist_from_inline
                                ~compute_pairwise_distance:pairwise_distance
                                ~dimension:dim
                            with
                            | Error (Hnsw_mvcc.Corrupted_data msg) ->
                                Hnsw_mvcc.rollback mvcc hnsw_txn;
                                Error
                                  (Corrupted_data ("dimension mismatch: " ^ msg))
                            | Error e ->
                                Hnsw_mvcc.rollback mvcc hnsw_txn;
                                Error
                                  (Storage_error (Hnsw_mvcc.error_to_string e))
                            | Ok slot_id -> (
                                match
                                  Types.wrap_lmdb_exn (fun () ->
                                      let key = Keys.encode_id_bs vector_id in
                                      let owner_value =
                                        Keys.encode_vector_owner_bs
                                          ~owner_kind:req.owner_kind
                                          ~owner_id:req.owner_id ~vector_tag_id
                                          ~file_offset
                                      in
                                      Lmdb.Map.set t.db.vector_owners ~txn key
                                        owner_value;
                                      let index_key =
                                        Keys.encode_vector_index_bs
                                          ~owner_kind:req.owner_kind
                                          ~owner_id:req.owner_id ~vector_tag_id
                                          ~vector_id
                                      in
                                      Lmdb.Map.set t.db.vector_index ~txn
                                        index_key Store.empty_bigstring;
                                      let slot_key =
                                        Keys.encode_hnsw_slot_key
                                          ~tag_id:vector_tag_id ~vector_id
                                      in
                                      Lmdb.Map.set t.db.hnsw_slots ~txn slot_key
                                        (Keys.encode_hnsw_slot_value slot_id))
                                with
                                | Error e ->
                                    Hnsw_mvcc.rollback mvcc hnsw_txn;
                                    Error e
                                | Ok () ->
                                    insert_all (vector_id :: acc) rest)))))
          in
          match insert_all [] requests with
          | Error e ->
              Hnsw_mvcc.rollback mvcc hnsw_txn;
              Error e
          | Ok ids -> (
              match Hnsw_mvcc.commit mvcc hnsw_txn with
              | Error e ->
                  Error (Storage_error (Hnsw_mvcc.error_to_string e))
              | Ok () ->
                  set_lmdb_hnsw_epoch t.db ~txn vector_tag
                    (Hnsw_mvcc.get_epoch mvcc);
                  Ok ids)))

let create_vector (t : t) ~txn ?(normalize = true) ?(metric = Cosine)
    ?hnsw_params (owner_kind : owner_kind) (owner_id : id) (vector_tag : string)
    (data : bigstring) : (vector_id, error) result =
  let* exists =
    match owner_kind with
    | Node -> node_exists t ~txn owner_id
    | Edge -> edge_exists t ~txn owner_id
  in
  if not exists then
    match owner_kind with
    | Node -> Error (Node_not_found owner_id)
    | Edge -> Error (Edge_not_found owner_id)
  else
    create_vector_internal t ~txn ~normalize ~metric ?hnsw_params owner_kind
      owner_id vector_tag data

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
        (match (slot_id_opt, Hashtbl.find_opt t.hnsw_mvcc vector_tag) with
        | Some slot_id, Some mvcc ->
            let table = Hnsw_mvcc.begin_read mvcc in
            Fun.protect
              ~finally:(fun () -> Hnsw_mvcc.end_read mvcc table)
              (fun () ->
                match Hnsw_mvcc.read_node_with_vec mvcc table ~slot_id with
                | Some node -> (
                    let deleted_node : Hnsw_page.node_data =
                      { node with deleted = true }
                    in
                    let hnsw_txn = Hnsw_mvcc.begin_write mvcc in
                    Hnsw_mvcc.write_node mvcc hnsw_txn ~slot_id deleted_node;
                    (if slot_id = Hnsw_mvcc.table_entry_point table then
                       let ep, level =
                         find_best_entry_point mvcc table ~exclude_slot:slot_id
                       in
                       Hnsw_mvcc.set_entry_point hnsw_txn ~entry_point:ep
                         ~max_level:level);
                    match Hnsw_mvcc.commit mvcc hnsw_txn with
                    | Ok () ->
                        set_lmdb_hnsw_epoch t.db ?txn vector_tag
                          (Hnsw_mvcc.get_epoch mvcc)
                    | Error _ -> Hnsw_mvcc.rollback mvcc hnsw_txn)
                | None -> ())
        | _ -> ());
        (try Lmdb.Map.remove t.db.hnsw_slots ?txn slot_key
         with Not_found | Lmdb.Not_found -> ());
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
  let node_meta_exists =
    try
      let _ = Lmdb.Map.get t.db.node_meta ?txn key in
      true
    with Not_found | Lmdb.Not_found -> false
  in
  if not node_meta_exists then Error (Node_not_found node_id)
  else
    try
      let* () = delete_vectors_for_owner t ?txn Node node_id in
      let* outbound_edges = get_outbound_edges t ?txn node_id () in
      let rec delete_edges = function
        | [] -> Ok ()
        | edge :: rest ->
            let* () = delete_vectors_for_owner t ?txn Edge edge.id in
            let* () = delete_edge_data t ?txn edge.id in
            delete_edges rest
      in
      let* () = delete_edges outbound_edges in
      let* inbound_edges = get_inbound_edges t ?txn node_id () in
      let* () = delete_edges inbound_edges in
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

let get_vectors (t : t) ?txn (owner_kind : owner_kind) (owner_id : id)
    ?vector_tag () : (vector_info list, error) result =
  get_vectors_for_owner_internal t ?txn owner_kind owner_id ?vector_tag ()

let knn_brute_force t ?txn ~metric ~k query =
  Knn.brute_force t.db ?txn ~metric ~k query

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
        let table = Hnsw_mvcc.begin_read mvcc in
        Fun.protect
          ~finally:(fun () -> Hnsw_mvcc.end_read mvcc table)
          (fun () ->
            if Hnsw_mvcc.table_entry_point table < 0 then Ok []
            else
              let dim = Array.length query in
              let index_dim = Hnsw_mvcc.get_dimension mvcc in
              if index_dim > 0 && dim <> index_dim then
                Error
                  (Storage_error
                     (Printf.sprintf
                        "query dimension mismatch: index has %d, query has %d"
                        index_dim dim))
              else
                let query_f32 = Float32_vec.of_array query in
                let query_f32, query_norm = Float32_vec.normalize query_f32 in
                let hnsw_mmap = Hnsw_mvcc.get_mmap mvcc in
                let dist_from_offset =
                  make_compute_distance_hnsw hnsw_mmap metric query_f32
                    query_norm dim
                in
                let dist_from_inline =
                  make_dist_from_inline metric query_f32 query_norm dim
                in

                let ctx =
                  Hnsw_mvcc.create_search_context mvcc table ~dist_from_offset
                    ~dist_from_inline ~overlay:None
                in
                let results = Hnsw_mvcc.search_mvcc ctx ~k ~ef in

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
                Ok results_with_info)

let rebuild_hnsw_index (t : t) ?(txn : rw_txn option)
    ?(hnsw_params = Hnsw.default_params) ~vector_tag () =
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
  (* Rw cursor for in-place deletion; can't use fold_prefix *)
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
              if has_prefix key then (
                Lmdb.Cursor.remove cursor;
                delete_matching ())
            with Lmdb.Not_found -> ()
          in
          try
            let key, _ = Lmdb.Cursor.seek_range cursor prefix in
            if has_prefix key then (
              Lmdb.Cursor.remove cursor;
              delete_matching ())
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
  match Hnsw_mvcc.create file_path ~metric ~params:hnsw_params () with
  | Error e -> Error (Storage_error (Hnsw_mvcc.error_to_string e))
  | Ok mvcc -> (
      let vectors = get_all_vectors () in
      if vectors = [] then (
        Hashtbl.replace t.hnsw_mvcc vector_tag mvcc;
        Ok ())
      else
        let dim =
          match List.hd vectors with
          | _, offset -> (
              match
                Vector_file.read_vector_with_header t.db.vector_file offset
              with
              | Ok (bs, _) -> Float32_vec.dim bs
              | Error _ -> 0)
        in
        if dim = 0 then (
          Hashtbl.replace t.hnsw_mvcc vector_tag mvcc;
          Ok ())
        else
          let batch_size = 3_000_000 in
          let hnsw_txn = ref (Hnsw_mvcc.begin_write mvcc) in
          let count = ref 0 in
          let pairwise_distance = make_pairwise_distance_inline metric dim in
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
                    | Ok (vec_bs, hdr) -> (
                        let query_f32, query_norm =
                          if Vector_file.is_normalized hdr then
                            (vec_bs, hdr.Vector_file.norm)
                          else Float32_vec.normalize vec_bs
                        in
                        let hnsw_mmap = Hnsw_mvcc.get_mmap mvcc in
                        let compute_distance =
                          make_compute_distance_hnsw hnsw_mmap metric query_f32
                            query_norm dim
                        in
                        let dist_from_inline =
                          make_dist_from_inline metric query_f32 query_norm dim
                        in
                        let normalized = Vector_file.is_normalized hdr in
                        let inline_vec =
                          build_inline_vec ~normalized vec_bs
                            hdr.Vector_file.norm
                        in
                        match
                          Hnsw_mvcc.insert_mvcc mvcc !hnsw_txn ~vector_id
                            ~inline_vec ~compute_distance ~dist_from_inline
                            ~compute_pairwise_distance:pairwise_distance
                            ~dimension:dim
                        with
                        | Error (Hnsw_mvcc.Corrupted_data msg) ->
                            Error
                              (Corrupted_data ("dimension mismatch: " ^ msg))
                        | Error e ->
                            Error (Storage_error (Hnsw_mvcc.error_to_string e))
                        | Ok slot_id ->
                            (match tag_id_opt with
                            | Some tag_id ->
                                let slot_key =
                                  Keys.encode_hnsw_slot_key ~tag_id ~vector_id
                                in
                                Lmdb.Map.set t.db.hnsw_slots ?txn slot_key
                                  (Keys.encode_hnsw_slot_value slot_id)
                            | None -> ());
                            incr count;
                            if !count mod batch_size = 0 then (
                              match Hnsw_mvcc.commit mvcc !hnsw_txn with
                              | Error e ->
                                  Error
                                    (Storage_error (Hnsw_mvcc.error_to_string e))
                              | Ok () ->
                                  hnsw_txn := Hnsw_mvcc.begin_write mvcc;
                                  Ok ())
                            else Ok ())))
              (Ok ()) vectors
          in
          match result with
          | Error _ ->
              Hnsw_mvcc.rollback mvcc !hnsw_txn;
              Hnsw_mvcc.close mvcc;
              result
          | Ok () -> (
              match Hnsw_mvcc.commit mvcc !hnsw_txn with
              | Error e ->
                  Hnsw_mvcc.close mvcc;
                  Error (Storage_error (Hnsw_mvcc.error_to_string e))
              | Ok () ->
                  (* store HNSW epoch for crash consistency *)
                  set_lmdb_hnsw_epoch t.db ?txn vector_tag
                    (Hnsw_mvcc.get_epoch mvcc);
                  Hashtbl.replace t.hnsw_mvcc vector_tag mvcc;
                  Ok ()))

module Schema_registry = Schema_registry
module Dynamic_reader = Dynamic_reader
module Filter = Filter

let register_schema_from_capnp t ~kind ~type_name ~capnp_path ~struct_name ?txn
    () =
  Schema_registry.register_schema_from_capnp t.db ~kind ~type_name ~capnp_path
    ~struct_name ?txn ()

let register_schema_from_fields t ~kind ~type_name ~data_word_count
    ~pointer_count ~fields ?txn () =
  Schema_registry.register_schema_from_fields t.db ~kind ~type_name
    ~data_word_count ~pointer_count ~fields ?txn ()

let get_schema t ?txn type_name = Schema_registry.get_schema t.db ?txn type_name
let load_all_schemas t = Schema_registry.load_all_schemas t.db

let lmdb_get_or_corrupted map ?txn key msg =
  try Ok (Lmdb.Map.get map ?txn key)
  with Not_found | Lmdb.Not_found -> Error (Corrupted_data msg)

let read_node_field t ?txn node_id field_name =
  let key = Keys.encode_id_bs node_id in
  let* exists = node_exists t ?txn node_id in
  if not exists then Error (Node_not_found node_id)
  else
    let* meta_bs =
      lmdb_get_or_corrupted t.db.node_meta ?txn key
        "node_meta missing for existing node"
    in
    let intern_id = Keys.decode_id_bs meta_bs in
    let* type_name =
      try Ok (Store.unintern t.db ?txn intern_id)
      with Not_found | Lmdb.Not_found ->
        Error (Corrupted_data "intern reverse lookup failed")
    in
    let* schema = get_schema t ?txn type_name in
    let* props_bs =
      lmdb_get_or_corrupted t.db.nodes ?txn key
        "node props missing for existing node"
    in
    Dynamic_reader.read_field_by_name props_bs schema field_name

let read_edge_field t ?txn edge_id field_name =
  let key = Keys.encode_id_bs edge_id in
  let* exists = edge_exists t ?txn edge_id in
  if not exists then Error (Edge_not_found edge_id)
  else
    let* props_bs =
      lmdb_get_or_corrupted t.db.edges ?txn key
        "edge props missing for existing edge"
    in
    let* meta_bs =
      lmdb_get_or_corrupted t.db.edge_meta ?txn key
        "edge_meta missing for existing edge"
    in
    let intern_id, _, _ = Keys.decode_edge_meta meta_bs in
    let* type_name =
      try Ok (Store.unintern t.db ?txn intern_id)
      with Not_found | Lmdb.Not_found ->
        Error (Corrupted_data "intern reverse lookup failed")
    in
    let* schema = get_schema t ?txn type_name in
    Dynamic_reader.read_field_by_name props_bs schema field_name

let get_node_props (t : t) ?txn (node_id : node_id) : (bigstring, error) result
    =
  let key = Keys.encode_id_bs node_id in
  try Ok (Lmdb.Map.get t.db.nodes ?txn key)
  with Not_found | Lmdb.Not_found -> Error (Node_not_found node_id)

let get_edge_props (t : t) ?txn (edge_id : edge_id) : (bigstring, error) result
    =
  let key = Keys.encode_id_bs edge_id in
  try Ok (Lmdb.Map.get t.db.edges ?txn key)
  with Not_found | Lmdb.Not_found -> Error (Edge_not_found edge_id)

let set_node_props (t : t) ?txn (node_id : node_id) (type_name : string)
    (data : bigstring) : (unit, error) result =
  let* exists = node_exists t ?txn node_id in
  if not exists then Error (Node_not_found node_id)
  else
    let* intern_id = Store.intern t.db ?txn type_name in
    Types.wrap_lmdb_exn (fun () ->
        let key = Keys.encode_id_bs node_id in
        Lmdb.Map.set t.db.node_meta ?txn key (Keys.encode_id_bs intern_id);
        Lmdb.Map.set t.db.nodes ?txn key data)

let set_edge_props (t : t) ?txn (edge_id : edge_id) (data : bigstring) :
    (unit, error) result =
  let key = Keys.encode_id_bs edge_id in
  let* exists = edge_exists t ?txn edge_id in
  if not exists then Error (Edge_not_found edge_id)
  else Types.wrap_lmdb_exn (fun () -> Lmdb.Map.set t.db.edges ?txn key data)

module Types = Types
module Hnsw_page = Hnsw_page
module Hnsw_mvcc = Hnsw_mvcc
module Hnsw = Hnsw
