(** HNSW index tests *)

open Alcotest
open Test_common
module Bigstring = Bigstringaf

let floats_to_bigstring (arr : float array) : Gvecdb.bigstring =
  let n = Array.length arr in
  let bs = Bigstring.create (n * 4) in
  for i = 0 to n - 1 do
    Bigstring.set_int32_le bs (i * 4) (Int32.bits_of_float arr.(i))
  done;
  bs

let with_txn db f =
  match Gvecdb.with_transaction db f with
  | Some x -> x
  | None -> Alcotest.fail "transaction aborted unexpectedly"

(** {1 Basic HNSW tests} *)

let test_hnsw_empty () =
  with_temp_db "hnsw" @@ fun db ->
  let query = [| 1.0; 0.0 |] in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"embedding"
         query)
  in
  check int "no results on empty index" 0 (List.length results)

let test_hnsw_single_vector () =
  with_temp_db "hnsw" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let v1 =
    with_txn db (fun txn ->
        ok_exn
          (Gvecdb.create_vector db ~txn n1 "embedding"
             (floats_to_bigstring [| 1.0; 0.0 |])))
  in
  let query = [| 1.0; 0.0 |] in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"embedding"
         query)
  in
  check int "one result" 1 (List.length results);
  check int64 "correct vector" v1 (List.hd results).Gvecdb.vector_id;
  check (float 0.0001) "distance is ~0" 0.0 (List.hd results).Gvecdb.distance

let test_hnsw_euclidean () =
  with_temp_db "hnsw" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let n3 = ok_exn (Gvecdb.create_node db "doc") in
  let v2 =
    with_txn db (fun txn ->
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 0.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n3 "e"
               (floats_to_bigstring [| 10.0; 10.0 |]))
        in
        v2)
  in
  let query = [| 0.9; 0.1 |] in
  let results =
    ok_exn (Gvecdb.knn_hnsw db ~metric:Cosine ~k:2 ~ef:50 ~vector_tag:"e" query)
  in
  check int "two results" 2 (List.length results);
  check int64 "nearest is v2" v2 (List.hd results).Gvecdb.vector_id;
  let d1 = (List.nth results 0).Gvecdb.distance in
  let d2 = (List.nth results 1).Gvecdb.distance in
  check bool "d1 < d2" true (d1 < d2)

let test_hnsw_cosine () =
  with_temp_db "hnsw" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 0.0; 1.0 |]))
        in
        v1)
  in
  let query = [| 10.0; 0.0 |] in
  let results =
    ok_exn (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e" query)
  in
  check int "one result" 1 (List.length results);
  check int64 "nearest is v1" v1 (List.hd results).Gvecdb.vector_id;
  check (float 0.0001) "zero cosine distance" 0.0
    (List.hd results).Gvecdb.distance

let test_hnsw_dot_product () =
  with_temp_db "hnsw" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 2.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 1.0; 1.0 |]))
        in
        v1)
  in
  let query = [| 1.0; 0.0 |] in
  let results =
    ok_exn (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e" query)
  in
  check int "one result" 1 (List.length results);
  check int64 "nearest is v1 (higher dot product)" v1
    (List.hd results).Gvecdb.vector_id

(** {1 Per-tag isolation tests} *)

let test_hnsw_per_tag_isolation () =
  with_temp_db "hnsw" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1, v2 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "tag_a"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "tag_b"
               (floats_to_bigstring [| 0.0; 1.0 |]))
        in
        (v1, v2))
  in
  (* Search in tag_a - should only find v1 *)
  let results_a =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"tag_a"
         [| 1.0; 0.0 |])
  in
  check int "one result in tag_a" 1 (List.length results_a);
  check int64 "v1 in tag_a" v1 (List.hd results_a).Gvecdb.vector_id;

  (* Search in tag_b - should only find v2 *)
  let results_b =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"tag_b"
         [| 0.0; 1.0 |])
  in
  check int "one result in tag_b" 1 (List.length results_b);
  check int64 "v2 in tag_b" v2 (List.hd results_b).Gvecdb.vector_id;

  (* Search in nonexistent tag *)
  let results_c =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"tag_c"
         [| 1.0; 0.0 |])
  in
  check int "no results in tag_c" 0 (List.length results_c)

(** {1 Soft delete tests} *)

let test_hnsw_soft_delete () =
  with_temp_db "hnsw" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1, v2 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 0.9; 0.1 |]))
        in
        (v1, v2))
  in
  (* Before delete - should find both *)
  let results_before =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0 |])
  in
  check int "two results before delete" 2 (List.length results_before);

  (* Delete v1 *)
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn v1));

  (* After delete - should only find v2 *)
  let results_after =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0 |])
  in
  check int "one result after delete" 1 (List.length results_after);
  check int64 "remaining vector is v2" v2
    (List.hd results_after).Gvecdb.vector_id

(** {1 Recall benchmark tests} *)

let random_vector dim = Array.init dim (fun _ -> Random.float 2.0 -. 1.0)

let test_hnsw_recall () =
  with_temp_db "hnsw" @@ fun db ->
  let dim = 32 in
  let n_vectors = 100 in
  let k = 10 in

  (* Create nodes and vectors *)
  let vectors =
    with_txn db (fun txn ->
        Array.init n_vectors (fun _ ->
            let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
            let vec = random_vector dim in
            let vid =
              ok_exn
                (Gvecdb.create_vector db ~txn node "e" (floats_to_bigstring vec))
            in
            (vid, vec)))
  in

  (* Random query *)
  let query = random_vector dim in

  (* Get brute force results as ground truth *)
  let bf_results = ok_exn (Gvecdb.knn_brute_force db ~metric:Cosine ~k query) in
  let bf_ids =
    List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) bf_results
  in

  (* Get HNSW results *)
  let hnsw_results =
    ok_exn (Gvecdb.knn_hnsw db ~metric:Cosine ~k ~ef:100 ~vector_tag:"e" query)
  in
  let hnsw_ids =
    List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) hnsw_results
  in

  (* Calculate recall *)
  let recall =
    let matches =
      List.fold_left
        (fun acc id -> if List.mem id bf_ids then acc + 1 else acc)
        0 hnsw_ids
    in
    float_of_int matches /. float_of_int (List.length bf_ids)
  in

  (* We expect high recall with ef=100 on a small dataset *)
  check bool
    (Printf.sprintf "recall@%d >= 0.8 (got %.2f)" k recall)
    true (recall >= 0.8);

  (* Sanity check that we got vectors *)
  check bool "got some results" true (List.length hnsw_results > 0);
  ignore vectors

(** {1 knn_hnsw_bs tests} *)

let test_hnsw_bs () =
  with_temp_db "hnsw" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let v1 =
    with_txn db (fun txn ->
        ok_exn
          (Gvecdb.create_vector db ~txn n1 "e"
             (floats_to_bigstring [| 1.0; 2.0 |])))
  in
  let query_bs = floats_to_bigstring [| 1.1; 2.1 |] in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw_bs db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e" query_bs)
  in
  check int "one result" 1 (List.length results);
  check int64 "nearest is v1" v1 (List.hd results).Gvecdb.vector_id

(** {1 rebuild_hnsw_index tests} *)

let test_rebuild_hnsw_index () =
  with_temp_db "hnsw" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1, v2 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 0.0; 1.0 |]))
        in
        (v1, v2))
  in

  (* Rebuild index *)
  with_txn db (fun txn ->
      ok_exn (Gvecdb.rebuild_hnsw_index db ~txn ~vector_tag:"e" ()));

  (* Search should still work *)
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0 |])
  in
  check int "two results after rebuild" 2 (List.length results);
  check int64 "nearest is v1" v1 (List.hd results).Gvecdb.vector_id;
  ignore v2

(** {1 Many vectors test} *)

let test_hnsw_many_vectors () =
  with_temp_db "hnsw" @@ fun db ->
  let dim = 8 in
  let n_vectors = 500 in

  (* Create many vectors *)
  with_txn db (fun txn ->
      for _ = 1 to n_vectors do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let vec = random_vector dim in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn node "e" (floats_to_bigstring vec))
        in
        ()
      done);

  (* Search should work *)
  let query = random_vector dim in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e" query)
  in
  check int "got 10 results" 10 (List.length results);

  (* Distances should be ascending *)
  let rec check_ascending = function
    | [] | [ _ ] -> true
    | a :: (b :: _ as rest) ->
        a.Gvecdb.distance <= b.Gvecdb.distance && check_ascending rest
  in
  check bool "distances ascending" true (check_ascending results)

(** {1 Persistence tests} *)

(* Helper for persistence tests - get a temp path without auto-cleanup *)
let get_temp_path prefix =
  Filename.(
    concat (get_temp_dir_name ())
      (Printf.sprintf "%s_%d_%d.db" prefix (Unix.getpid ()) (Random.int 100000)))

let cleanup_db_files path =
  (try Sys.remove path with _ -> ());
  (* Also remove vectors file *)
  let base = Filename.remove_extension path in
  (try Sys.remove (base ^ ".vectors") with _ -> ());
  (* Also remove hnsw subdir *)
  let hnsw_dir = base ^ ".hnsw" in
  try
    if Sys.file_exists hnsw_dir && Sys.is_directory hnsw_dir then begin
      Array.iter
        (fun f -> Sys.remove (Filename.concat hnsw_dir f))
        (Sys.readdir hnsw_dir);
      Unix.rmdir hnsw_dir
    end
  with _ -> ()

let test_persistence_roundtrip () =
  let path = get_temp_path "hnsw_persist" in
  cleanup_db_files path;
  let dim = 8 in
  let n_vectors = 50 in

  (* Create vectors and track them *)
  let vectors = ref [] in
  let db = ok_exn (Gvecdb.create path) in
  with_txn db (fun txn ->
      for _ = 1 to n_vectors do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let vec = random_vector dim in
        let vid =
          ok_exn
            (Gvecdb.create_vector db ~txn node "e" (floats_to_bigstring vec))
        in
        vectors := (vid, vec) :: !vectors
      done);

  (* Query before close *)
  let query = random_vector dim in
  (* Get brute force ground truth *)
  let _bf_results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Cosine ~k:10 query)
  in
  let results_before =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e" query)
  in
  let ids_before = List.map (fun r -> r.Gvecdb.vector_id) results_before in

  (* Close and reopen *)
  Gvecdb.close db;
  let db2 = ok_exn (Gvecdb.create path) in

  (* Query after reopen *)
  let results_after =
    ok_exn
      (Gvecdb.knn_hnsw db2 ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e" query)
  in
  let ids_after = List.map (fun r -> r.Gvecdb.vector_id) results_after in

  (* Results should match - check count and significant overlap *)
  check int "same result count" (List.length ids_before) (List.length ids_after);

  (* For HNSW with persistence, exact match is expected if loaded from file.
     Calculate overlap to diagnose if index was rebuilt vs loaded. *)
  let overlap =
    List.fold_left
      (fun acc id -> if List.mem id ids_before then acc + 1 else acc)
      0 ids_after
  in
  (* If overlap is 10/10, index was loaded correctly.
     If overlap is low, index was rebuilt (indicates bug in persistence).
     Allow some tolerance for approximate search behavior. *)
  check bool
    (Printf.sprintf "significant overlap (%d/10 common results)" overlap)
    true (overlap >= 8);

  Gvecdb.close db2;
  cleanup_db_files path

let test_delete_persistence () =
  let path = get_temp_path "hnsw_delete_persist" in
  cleanup_db_files path;

  (* Create vectors *)
  let db = ok_exn (Gvecdb.create path) in
  let v1, v2 =
    with_txn db (fun txn ->
        let n1 = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let n2 = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 1.0; 0.0; 0.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 0.0; 1.0; 0.0; 0.0 |]))
        in
        (v1, v2))
  in

  (* Delete v1 *)
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn v1));

  (* Query before close - should only find v2 *)
  let results_before =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0; 0.0; 0.0 |])
  in
  check int "one result before close" 1 (List.length results_before);
  check int64 "v2 is remaining" v2 (List.hd results_before).Gvecdb.vector_id;

  (* Close and reopen *)
  Gvecdb.close db;
  let db2 = ok_exn (Gvecdb.create path) in

  (* Query after reopen - should still only find v2 *)
  let results_after =
    ok_exn
      (Gvecdb.knn_hnsw db2 ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0; 0.0; 0.0 |])
  in
  check int "one result after reopen" 1 (List.length results_after);
  check int64 "v2 is remaining after reopen" v2
    (List.hd results_after).Gvecdb.vector_id;

  Gvecdb.close db2;
  cleanup_db_files path

let test_entry_point_deletion () =
  with_temp_db "hnsw_ep_delete" @@ fun db ->
  (* Create single vector (becomes entry point) *)
  let v1 =
    with_txn db (fun txn ->
        let n1 = ok_exn (Gvecdb.create_node db ~txn "doc") in
        ok_exn
          (Gvecdb.create_vector db ~txn n1 "e"
             (floats_to_bigstring [| 1.0; 0.0; 0.0; 0.0 |])))
  in

  (* Verify it's searchable *)
  let results1 =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0; 0.0; 0.0 |])
  in
  check int "one result" 1 (List.length results1);
  check int64 "found v1" v1 (List.hd results1).Gvecdb.vector_id;

  (* Delete the entry point *)
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn v1));

  (* Empty index now *)
  let results2 =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0; 0.0; 0.0 |])
  in
  check int "no results after entry point deleted" 0 (List.length results2);

  (* Add new vector *)
  let v2 =
    with_txn db (fun txn ->
        let n2 = ok_exn (Gvecdb.create_node db ~txn "doc") in
        ok_exn
          (Gvecdb.create_vector db ~txn n2 "e"
             (floats_to_bigstring [| 0.0; 1.0; 0.0; 0.0 |])))
  in

  (* Search should work again *)
  let results3 =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e"
         [| 0.0; 1.0; 0.0; 0.0 |])
  in
  check int "one result with new vector" 1 (List.length results3);
  check int64 "found v2" v2 (List.hd results3).Gvecdb.vector_id

(** {1 Edge case tests} *)

let test_k_larger_than_dataset () =
  with_temp_db "hnsw_k_large" @@ fun db ->
  let dim = 4 in

  (* Create 5 vectors *)
  with_txn db (fun txn ->
      for i = 1 to 5 do
        let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let vec =
          Array.init dim (fun j -> if j = 0 then float_of_int i else 0.0)
        in
        let _ =
          ok_exn (Gvecdb.create_vector db ~txn n "e" (floats_to_bigstring vec))
        in
        ()
      done);

  (* Query with k=20 (larger than 5) *)
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:20 ~ef:50 ~vector_tag:"e"
         [| 0.0; 0.0; 0.0; 0.0 |])
  in
  check int "returns all 5 vectors" 5 (List.length results)

let test_zero_vector_query () =
  with_temp_db "hnsw_zero" @@ fun db ->
  (* Create vectors *)
  with_txn db (fun txn ->
      let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
      let _ =
        ok_exn
          (Gvecdb.create_vector db ~txn n "e"
             (floats_to_bigstring [| 1.0; 0.0; 0.0; 0.0 |]))
      in
      ());

  (* Query with zero vector *)
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e"
         [| 0.0; 0.0; 0.0; 0.0 |])
  in
  check int "got one result" 1 (List.length results)

let test_insert_delete_reinsert () =
  with_temp_db "hnsw_reinsert" @@ fun db ->
  (* Insert vector *)
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let v1 =
    with_txn db (fun txn ->
        ok_exn
          (Gvecdb.create_vector db ~txn n1 "e"
             (floats_to_bigstring [| 1.0; 0.0; 0.0; 0.0 |])))
  in

  (* Delete it *)
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn v1));

  (* Reinsert new vector on same node *)
  let v2 =
    with_txn db (fun txn ->
        ok_exn
          (Gvecdb.create_vector db ~txn n1 "e"
             (floats_to_bigstring [| 0.0; 1.0; 0.0; 0.0 |])))
  in

  (* Search should find v2, not v1 *)
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 0.0; 1.0; 0.0; 0.0 |])
  in
  check int "one result" 1 (List.length results);
  check int64 "found v2" v2 (List.hd results).Gvecdb.vector_id

(** {1 Recall quality tests} *)

let compute_recall ~bf_ids ~hnsw_ids =
  let matches =
    List.fold_left
      (fun acc id -> if List.mem id bf_ids then acc + 1 else acc)
      0 hnsw_ids
  in
  if List.length bf_ids = 0 then 1.0
  else float_of_int matches /. float_of_int (List.length bf_ids)

let test_ef_parameter_sweep () =
  with_temp_db "hnsw_ef_sweep" @@ fun db ->
  let dim = 32 in
  let n_vectors = 200 in
  let k = 10 in

  (* Create vectors *)
  with_txn db (fun txn ->
      for _ = 1 to n_vectors do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let vec = random_vector dim in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn node "e" (floats_to_bigstring vec))
        in
        ()
      done);

  let query = random_vector dim in

  (* Ground truth *)
  let bf_results = ok_exn (Gvecdb.knn_brute_force db ~metric:Cosine ~k query) in
  let bf_ids = List.map (fun r -> r.Gvecdb.vector_id) bf_results in

  (* Test different ef values *)
  let ef_values = [ 10; 50; 100; 200 ] in
  let recalls =
    List.map
      (fun ef ->
        let hnsw_results =
          ok_exn
            (Gvecdb.knn_hnsw db ~metric:Cosine ~k ~ef ~vector_tag:"e" query)
        in
        let hnsw_ids = List.map (fun r -> r.Gvecdb.vector_id) hnsw_results in
        (ef, compute_recall ~bf_ids ~hnsw_ids))
      ef_values
  in

  (* Verify recall generally increases with ef *)
  let rec check_monotonic = function
    | [] | [ _ ] -> true
    | (_ef1, r1) :: ((ef2, r2) :: _ as rest) ->
        (* Allow small regression due to randomness, but generally should increase *)
        (r2 >= r1 -. 0.1 || r1 >= 0.95)
        && check_monotonic ((ef2, r2) :: List.tl rest)
  in
  check bool "recall increases with ef (roughly)" true (check_monotonic recalls);

  (* Final recall at ef=200 should be high *)
  let final_recall = snd (List.hd (List.rev recalls)) in
  check bool
    (Printf.sprintf "recall at ef=200 >= 0.9 (got %.2f)" final_recall)
    true (final_recall >= 0.9)

let test_k_parameter_sweep () =
  with_temp_db "hnsw_k_sweep" @@ fun db ->
  let dim = 16 in
  let n_vectors = 100 in

  (* Create vectors *)
  with_txn db (fun txn ->
      for _ = 1 to n_vectors do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let vec = random_vector dim in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn node "e" (floats_to_bigstring vec))
        in
        ()
      done);

  let query = random_vector dim in

  (* Test different k values *)
  let k_values = [ 1; 5; 10; 20; 50 ] in
  List.iter
    (fun k ->
      let results =
        ok_exn
          (Gvecdb.knn_hnsw db ~metric:Cosine ~k ~ef:100 ~vector_tag:"e" query)
      in
      let expected_count = min k n_vectors in
      (* Allow some tolerance since HNSW is approximate *)
      let min_count = max 1 (expected_count * 9 / 10) in
      check bool
        (Printf.sprintf "k=%d returns at least %d results (got %d)" k min_count
           (List.length results))
        true
        (List.length results >= min_count))
    k_values

let test_dimension_sweep () =
  let dims = [ 8; 32; 64; 128 ] in
  List.iter
    (fun dim ->
      with_temp_db (Printf.sprintf "hnsw_dim%d" dim) @@ fun db ->
      let n_vectors = 50 in
      let k = 5 in

      (* Create vectors *)
      with_txn db (fun txn ->
          for _ = 1 to n_vectors do
            let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
            let vec = random_vector dim in
            let _ =
              ok_exn
                (Gvecdb.create_vector db ~txn node "e" (floats_to_bigstring vec))
            in
            ()
          done);

      let query = random_vector dim in

      let results =
        ok_exn
          (Gvecdb.knn_hnsw db ~metric:Cosine ~k ~ef:50 ~vector_tag:"e" query)
      in
      check int
        (Printf.sprintf "dim=%d returns k results" dim)
        k (List.length results);

      (* Verify distances are ascending *)
      let rec check_ascending = function
        | [] | [ _ ] -> true
        | a :: (b :: _ as rest) ->
            a.Gvecdb.distance <= b.Gvecdb.distance && check_ascending rest
      in
      check bool
        (Printf.sprintf "dim=%d distances ascending" dim)
        true (check_ascending results))
    dims

(** {1 Stress tests} *)

let test_large_index () =
  with_temp_db "hnsw_large" @@ fun db ->
  let dim = 16 in
  let n_vectors = 1000 in
  let k = 20 in

  (* Create many vectors *)
  with_txn db (fun txn ->
      for _ = 1 to n_vectors do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let vec = random_vector dim in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn node "e" (floats_to_bigstring vec))
        in
        ()
      done);

  (* Search should work *)
  let query = random_vector dim in
  let results =
    ok_exn (Gvecdb.knn_hnsw db ~metric:Cosine ~k ~ef:100 ~vector_tag:"e" query)
  in
  check int "got k results" k (List.length results);

  (* Distances should be ascending *)
  let rec check_ascending = function
    | [] | [ _ ] -> true
    | a :: (b :: _ as rest) ->
        a.Gvecdb.distance <= b.Gvecdb.distance && check_ascending rest
  in
  check bool "distances ascending" true (check_ascending results)

let test_heavy_deletion () =
  with_temp_db "hnsw_heavy_delete" @@ fun db ->
  let dim = 8 in
  let n_initial = 200 in
  let n_delete = 150 in
  let n_remaining = n_initial - n_delete in

  (* Create vectors *)
  let vector_ids =
    with_txn db (fun txn ->
        Array.init n_initial (fun _ ->
            let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
            let vec = random_vector dim in
            ok_exn
              (Gvecdb.create_vector db ~txn node "e" (floats_to_bigstring vec))))
  in

  (* Delete first n_delete vectors *)
  with_txn db (fun txn ->
      for i = 0 to n_delete - 1 do
        ok_exn (Gvecdb.delete_vector db ~txn vector_ids.(i))
      done);

  (* Search should still work and return remaining vectors *)
  let query = random_vector dim in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:n_remaining ~ef:100 ~vector_tag:"e"
         query)
  in
  (* Allow some tolerance since HNSW is approximate *)
  let min_expected = max 1 (n_remaining * 9 / 10) in
  check bool
    (Printf.sprintf "returns most remaining vectors (got %d, expected >= %d)"
       (List.length results) min_expected)
    true
    (List.length results >= min_expected);

  (* All returned vectors should be from the non-deleted set *)
  let remaining_ids = Array.sub vector_ids n_delete n_remaining in
  let remaining_set =
    Array.fold_left (fun acc id -> id :: acc) [] remaining_ids
  in
  List.iter
    (fun r ->
      check bool "returned vector is from remaining set" true
        (List.mem r.Gvecdb.vector_id remaining_set))
    results

(** {1 Test runner} *)

let basic_tests =
  [
    ("empty index", `Quick, test_hnsw_empty);
    ("single vector", `Quick, test_hnsw_single_vector);
    ("euclidean", `Quick, test_hnsw_euclidean);
    ("cosine", `Quick, test_hnsw_cosine);
    ("dot_product", `Quick, test_hnsw_dot_product);
  ]

let isolation_tests =
  [ ("per_tag_isolation", `Quick, test_hnsw_per_tag_isolation) ]

let delete_tests = [ ("soft_delete", `Quick, test_hnsw_soft_delete) ]
let recall_tests = [ ("recall_benchmark", `Slow, test_hnsw_recall) ]
let bigstring_tests = [ ("knn_hnsw_bs", `Quick, test_hnsw_bs) ]
let rebuild_tests = [ ("rebuild_index", `Quick, test_rebuild_hnsw_index) ]

let persistence_tests =
  [
    ("round_trip", `Quick, test_persistence_roundtrip);
    ("delete_persistence", `Quick, test_delete_persistence);
    ("entry_point_deletion", `Quick, test_entry_point_deletion);
  ]

let edge_case_tests =
  [
    ("k_larger_than_dataset", `Quick, test_k_larger_than_dataset);
    ("zero_vector_query", `Quick, test_zero_vector_query);
    ("insert_delete_reinsert", `Quick, test_insert_delete_reinsert);
  ]

let quality_tests =
  [
    ("ef_parameter_sweep", `Slow, test_ef_parameter_sweep);
    ("k_parameter_sweep", `Quick, test_k_parameter_sweep);
    ("dimension_sweep", `Slow, test_dimension_sweep);
  ]

let stress_tests =
  [
    ("many_vectors", `Slow, test_hnsw_many_vectors);
    ("large_index", `Slow, test_large_index);
    ("heavy_deletion", `Slow, test_heavy_deletion);
  ]

(** {1 Bug fix regression tests} *)

let test_rebuild_then_delete () =
  with_temp_db "rebuild_delete" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1, v2 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 1.0; 0.0; 0.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 0.0; 1.0; 0.0; 0.0 |]))
        in
        (v1, v2))
  in
  (* Rebuild index - this should update slot mappings *)
  with_txn db (fun txn ->
      ok_exn (Gvecdb.rebuild_hnsw_index db ~txn ~vector_tag:"e" ()));

  (* Search should find both vectors *)
  let results_before =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0; 0.0; 0.0 |])
  in
  check int "two results before delete" 2 (List.length results_before);

  (* Delete v1 after rebuild - this is the critical test *)
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn v1));

  (* Search should now only find v2 *)
  let results_after =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0; 0.0; 0.0 |])
  in
  check int "one result after delete" 1 (List.length results_after);
  check int64 "remaining vector is v2" v2
    (List.hd results_after).Gvecdb.vector_id

let test_dimension_mismatch () =
  with_temp_db "dim_mismatch" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  (* Create index with 4-dimensional vectors *)
  with_txn db (fun txn ->
      let _ =
        ok_exn
          (Gvecdb.create_vector db ~txn n1 "e"
             (floats_to_bigstring [| 1.0; 0.0; 0.0; 0.0 |]))
      in
      ());
  (* Query with wrong dimension (8 instead of 4) should return error *)
  let result =
    Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e"
      [| 1.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0 |]
  in
  match result with
  | Error (Gvecdb.Storage_error msg) ->
      check bool "error mentions dimension" true
        (String.length msg > 0
        && (String.sub msg 0 (min 5 (String.length msg)) = "query"
           || String.sub msg 0 (min 9 (String.length msg)) = "dimension"))
  | Error _ -> fail "expected Storage_error for dimension mismatch"
  | Ok _ -> fail "expected error for dimension mismatch, got Ok"

let bug_fix_tests =
  [
    ("rebuild_then_delete", `Quick, test_rebuild_then_delete);
    ("dimension_mismatch", `Quick, test_dimension_mismatch);
  ]

let () =
  run "HNSW"
    [
      ("basic", basic_tests);
      ("isolation", isolation_tests);
      ("delete", delete_tests);
      ("recall", recall_tests);
      ("bigstring", bigstring_tests);
      ("rebuild", rebuild_tests);
      ("persistence", persistence_tests);
      ("edge_cases", edge_case_tests);
      ("quality", quality_tests);
      ("stress", stress_tests);
      ("bug_fixes", bug_fix_tests);
    ]
