open Types

let brute_force (db : t) ?txn ~(metric : distance_metric) ~(k : int)
    (query : float array) : (knn_result list, error) result =
  if k <= 0 then Ok []
  else
    let query_f32 = Float32_vec.of_array query in
    let query_f32, query_norm = Float32_vec.normalize query_f32 in
    let dim = Array.length query in
    let metric_int = Types.metric_to_int metric in
    let txn_ro = Option.map (fun t -> (t :> [ `Read ] Lmdb.Txn.t)) txn in
    try
      let topk = Int_topk.create k in
      let meta = Hashtbl.create (k * 2) in
      Lmdb.Cursor.go Lmdb.Ro ?txn:txn_ro db.vector_owners (fun cursor ->
          let process key value =
            let vid = Keys.decode_id_bs key in
            let owner_kind, owner_id, tag_id, offset =
              Keys.decode_vector_owner_bs value
            in
            let off = Int64.to_int offset in
            let vf = db.vector_file in
            if off >= Vector_file.file_header_size
               && off + Vector_file.vec_header_size <= vf.file_size
            then begin
              let dist =
                Float32_vec.dist_from_header vf.mmap ~vec_off:off query_f32
                  ~query_norm ~metric:metric_int ~dim
              in
              if Float.is_finite dist then
                if (not (Int_topk.is_full topk))
                   || dist < Int_topk.worst_dist topk
                then begin
                  let vid_int = Int64.to_int vid in
                  Int_topk.insert topk dist vid_int;
                  Hashtbl.replace meta vid_int (vid, owner_kind, owner_id, tag_id)
                end
            end
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
        Int_topk.to_sorted_list topk
        |> List.filter_map (fun (dist, vid_int) ->
            match Hashtbl.find_opt meta vid_int with
            | None -> None
            | Some (vid, ok, oid, tid) ->
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
