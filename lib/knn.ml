open Types

(* distance for normalized vectors. returns nan on dimension mismatch *)
let compute_distance_normalized metric normalized_query query_norm vec_bs
    vec_norm dim =
  if not (Float32_vec.has_dim vec_bs dim) then nan
  else
    let norm_dot = Float32_vec.dot_with_array vec_bs normalized_query in
    match metric with
    | Cosine -> 1.0 -. norm_dot
    | Euclidean ->
        (* ||a-b||^2 = ||a||^2 + ||b||^2 - 2*dot(a,b) *)
        let dot = query_norm *. vec_norm *. norm_dot in
        let query_norm_sq = query_norm *. query_norm in
        let vec_norm_sq = vec_norm *. vec_norm in
        Float.max 0.0 (query_norm_sq +. vec_norm_sq -. (2.0 *. dot))
    | DotProduct -> -.(query_norm *. vec_norm *. norm_dot)

(* distance for non-normalized vectors. returns nan on dimension mismatch *)
let compute_distance_raw metric query query_norm_sq vec_bs vec_norm dim =
  if not (Float32_vec.has_dim vec_bs dim) then nan
  else
    let dot = Float32_vec.dot_with_array vec_bs query in
    let vec_norm_sq = vec_norm *. vec_norm in
    match metric with
    | Cosine ->
        let query_norm = sqrt query_norm_sq in
        let denom = query_norm *. vec_norm in
        if denom = 0.0 then 1.0 else 1.0 -. (dot /. denom)
    | Euclidean ->
        Float.max 0.0 (query_norm_sq +. vec_norm_sq -. (2.0 *. dot))
    | DotProduct -> -.dot

let array_norm_sq arr = Array.fold_left (fun acc x -> acc +. (x *. x)) 0.0 arr

let normalize_array arr =
  let norm_sq = array_norm_sq arr in
  let norm = sqrt norm_sq in
  if norm > 0.0 then begin
    let inv_norm = 1.0 /. norm in
    for i = 0 to Array.length arr - 1 do
      arr.(i) <- arr.(i) *. inv_norm
    done
  end;
  norm

(* early rejection based on norm bounds - only useful for euclidean *)
let should_skip_by_norm metric query_norm vec_norm worst_dist =
  match metric with
  | Euclidean ->
      let norm_diff = query_norm -. vec_norm in
      norm_diff *. norm_diff > worst_dist
  | Cosine | DotProduct -> false

let brute_force (db : t) ?txn ~(metric : distance_metric) ~(k : int)
    (query : float array) : (knn_result list, error) result =
  if k <= 0 then Ok []
  else
    let normalized_query = Array.copy query in
    let query_norm = normalize_array normalized_query in
    let query_norm_sq = query_norm *. query_norm in
    let dim = Array.length query in
    let txn_ro = Option.map (fun t -> (t :> [ `Read ] Lmdb.Txn.t)) txn in
    try
      let heap = Topk.create k in
      Lmdb.Cursor.go Lmdb.Ro ?txn:txn_ro db.vector_owners (fun cursor ->
          let process key value =
            let vid = Keys.decode_id_bs key in
            let owner_kind, owner_id, tag_id, offset =
              Keys.decode_vector_owner_bs value
            in
            match Vector_file.read_vector_with_header db.vector_file offset with
            | Error _ -> ()
            | Ok (vec_bs, header) ->
                let vec_norm = header.Vector_file.norm in
                let is_normalized = Vector_file.is_normalized header in
                if
                  (not (Topk.is_full heap))
                  || not
                       (should_skip_by_norm metric query_norm vec_norm
                          (Topk.worst_dist heap))
                then
                  let dist =
                    if is_normalized then
                      compute_distance_normalized metric normalized_query
                        query_norm vec_bs vec_norm dim
                    else
                      compute_distance_raw metric query query_norm_sq vec_bs
                        vec_norm dim
                  in
                  if Float.is_finite dist then
                    Topk.insert heap dist (vid, owner_kind, owner_id, tag_id)
          in
          let rec scan () =
            match Lmdb.Cursor.next cursor with
            | k, v ->
                process k v;
                scan ()
            | exception Lmdb.Not_found -> ()
          in
          match Lmdb.Cursor.first cursor with
          | k, v ->
              process k v;
              scan ()
          | exception Lmdb.Not_found -> ());
      let results =
        Topk.to_sorted_list heap
        |> List.filter_map (fun (dist, (vid, ok, oid, tid)) ->
            try
              let tag = Store.unintern db ?txn tid in
              Some
                ({
                   vector_id = vid;
                   owner_kind = ok;
                   owner_id = oid;
                   vector_tag = tag;
                   distance = dist;
                 }
                  : knn_result)
            with Not_found | Lmdb.Not_found -> None)
      in
      Ok results
    with
    | Not_found | Lmdb.Not_found -> Ok []
    | Lmdb.Map_full -> Error Storage_full
    | Lmdb.Error code ->
        Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))

let brute_force_bs db ?txn ~metric ~k query =
  brute_force db ?txn ~metric ~k (Float32_vec.to_array query)
