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

let random_vector dim = Array.init dim (fun _ -> Random.float 2.0 -. 1.0)

let get_temp_path prefix =
  Filename.(
    concat (get_temp_dir_name ())
      (Printf.sprintf "%s_%d_%d.db" prefix (Unix.getpid ()) (Random.int 100000)))

let cleanup_hnsw_files path =
  (try Sys.remove path with _ -> ());
  let base = Filename.remove_extension path in
  (try Sys.remove (base ^ ".vectors") with _ -> ());
  let hnsw_dir = base ^ ".hnsw" in
  try
    if Sys.file_exists hnsw_dir && Sys.is_directory hnsw_dir then (
      Array.iter
        (fun f -> Sys.remove (Filename.concat hnsw_dir f))
        (Sys.readdir hnsw_dir);
      Unix.rmdir hnsw_dir)
  with _ -> ()

let test_hnsw_basic () =
  with_temp_db "hnsw" @@ fun db ->
  let results_empty =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0 |])
  in
  check int "empty index" 0 (List.length results_empty);
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n1 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n2 "e"
               (floats_to_bigstring [| 0.0; 1.0 |]))
        in
        v1)
  in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e"
         [| 10.0; 0.0 |])
  in
  check int "one result" 1 (List.length results);
  check int64 "nearest is v1" v1 (List.hd results).Gvecdb.vector_id;
  check (float 0.0001) "zero distance" 0.0 (List.hd results).distance

let test_per_tag_isolation () =
  with_temp_db "hnsw" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1, v2 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n1 "tag_a"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n2 "tag_b"
               (floats_to_bigstring [| 0.0; 1.0 |]))
        in
        (v1, v2))
  in
  let res_a =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"tag_a"
         [| 1.0; 0.0 |])
  in
  check int "one in tag_a" 1 (List.length res_a);
  check int64 "v1 in tag_a" v1 (List.hd res_a).vector_id;
  let res_b =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"tag_b"
         [| 0.0; 1.0 |])
  in
  check int "one in tag_b" 1 (List.length res_b);
  check int64 "v2 in tag_b" v2 (List.hd res_b).vector_id;
  let res_c =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"tag_c"
         [| 1.0; 0.0 |])
  in
  check int "empty tag_c" 0 (List.length res_c)

let test_soft_delete () =
  with_temp_db "hnsw" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1, v2 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n1 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n2 "e"
               (floats_to_bigstring [| 0.9; 0.1 |]))
        in
        (v1, v2))
  in
  check int "two before" 2
    (List.length
       (ok_exn
          (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
             [| 1.0; 0.0 |])));
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn v1));
  let after =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0 |])
  in
  check int "one after" 1 (List.length after);
  check int64 "v2 remaining" v2 (List.hd after).vector_id

let test_recall () =
  with_temp_db "hnsw" @@ fun db ->
  let dim = 32 in
  with_txn db (fun txn ->
      for _ = 1 to 200 do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node node "e"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);
  let query = random_vector dim in
  let k = 10 in
  let bf = ok_exn (Gvecdb.knn_brute_force db ~metric:Cosine ~k query) in
  let bf_ids = List.map (fun r -> r.Gvecdb.vector_id) bf in
  let hnsw =
    ok_exn (Gvecdb.knn_hnsw db ~metric:Cosine ~k ~ef:100 ~vector_tag:"e" query)
  in
  let hnsw_ids = List.map (fun r -> r.Gvecdb.vector_id) hnsw in
  let matches =
    List.fold_left
      (fun acc id -> if List.mem id bf_ids then acc + 1 else acc)
      0 hnsw_ids
  in
  let recall = float_of_int matches /. float_of_int (List.length bf_ids) in
  check bool
    (Printf.sprintf "recall >= 0.8 (got %.2f)" recall)
    true (recall >= 0.8)

let test_rebuild_index () =
  with_temp_db "hnsw" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1, v2 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n1 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n2 "e"
               (floats_to_bigstring [| 0.0; 1.0 |]))
        in
        (v1, v2))
  in
  with_txn db (fun txn ->
      ok_exn (Gvecdb.rebuild_hnsw_index db ~txn ~vector_tag:"e" ()));
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0 |])
  in
  check int "two after rebuild" 2 (List.length results);
  check int64 "nearest is v1" v1 (List.hd results).vector_id;
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn v1));
  let after =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e"
         [| 1.0; 0.0 |])
  in
  check int "one after rebuild+delete" 1 (List.length after);
  check int64 "v2 remaining" v2 (List.hd after).vector_id

let test_persistence () =
  let path = get_temp_path "hnsw_persist" in
  cleanup_hnsw_files path;
  let dim = 8 in
  let db = ok_exn (Gvecdb.create path) in
  with_txn db (fun txn ->
      for _ = 1 to 50 do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node node "e"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);
  let query = random_vector dim in
  let before =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e" query)
  in
  let ids_before = List.map (fun r -> r.Gvecdb.vector_id) before in
  Gvecdb.close db;
  let db2 = ok_exn (Gvecdb.create path) in
  let after =
    ok_exn
      (Gvecdb.knn_hnsw db2 ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"e" query)
  in
  let ids_after = List.map (fun r -> r.Gvecdb.vector_id) after in
  let overlap =
    List.fold_left
      (fun acc id -> if List.mem id ids_before then acc + 1 else acc)
      0 ids_after
  in
  check bool (Printf.sprintf "overlap >= 8 (got %d)" overlap) true (overlap >= 8);
  Gvecdb.close db2;
  cleanup_hnsw_files path

let test_entry_point_deletion () =
  with_temp_db "hnsw_ep" @@ fun db ->
  let v1 =
    with_txn db (fun txn ->
        let n1 = ok_exn (Gvecdb.create_node db ~txn "doc") in
        ok_exn
          (Gvecdb.create_vector db ~txn Node n1 "e"
             (floats_to_bigstring [| 1.0; 0.0; 0.0; 0.0 |])))
  in
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn v1));
  check int "empty after ep delete" 0
    (List.length
       (ok_exn
          (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e"
             [| 1.0; 0.0; 0.0; 0.0 |])));
  let v2 =
    with_txn db (fun txn ->
        let n2 = ok_exn (Gvecdb.create_node db ~txn "doc") in
        ok_exn
          (Gvecdb.create_vector db ~txn Node n2 "e"
             (floats_to_bigstring [| 0.0; 1.0; 0.0; 0.0 |])))
  in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e"
         [| 0.0; 1.0; 0.0; 0.0 |])
  in
  check int64 "found v2" v2 (List.hd results).vector_id

let test_ef_sweep () =
  with_temp_db "hnsw_ef" @@ fun db ->
  let dim = 32 in
  with_txn db (fun txn ->
      for _ = 1 to 200 do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node node "e"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);
  let query = random_vector dim in
  let k = 10 in
  let bf = ok_exn (Gvecdb.knn_brute_force db ~metric:Cosine ~k query) in
  let bf_ids = List.map (fun r -> r.Gvecdb.vector_id) bf in
  let recall_at ef =
    let hnsw =
      ok_exn (Gvecdb.knn_hnsw db ~metric:Cosine ~k ~ef ~vector_tag:"e" query)
    in
    let hnsw_ids = List.map (fun r -> r.Gvecdb.vector_id) hnsw in
    let matches =
      List.fold_left
        (fun acc id -> if List.mem id bf_ids then acc + 1 else acc)
        0 hnsw_ids
    in
    float_of_int matches /. float_of_int (List.length bf_ids)
  in
  let r10 = recall_at 10 in
  let r200 = recall_at 200 in
  check bool
    (Printf.sprintf "recall improves: ef=10 %.2f -> ef=200 %.2f" r10 r200)
    true
    (r200 >= r10 -. 0.1);
  check bool
    (Printf.sprintf "recall@200 >= 0.9 (got %.2f)" r200)
    true (r200 >= 0.9)

let test_dimension_sweep () =
  List.iter
    (fun dim ->
      with_temp_db (Printf.sprintf "hnsw_dim%d" dim) @@ fun db ->
      with_txn db (fun txn ->
          for _ = 1 to 50 do
            let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
            let _ =
              ok_exn
                (Gvecdb.create_vector db ~txn Node node "e"
                   (floats_to_bigstring (random_vector dim)))
            in
            ()
          done);
      let results =
        ok_exn
          (Gvecdb.knn_hnsw db ~metric:Cosine ~k:5 ~ef:50 ~vector_tag:"e"
             (random_vector dim))
      in
      check int (Printf.sprintf "dim=%d returns k" dim) 5 (List.length results);
      let rec ascending = function
        | [] | [ _ ] -> true
        | a :: (b :: _ as rest) ->
            a.Gvecdb.distance <= b.distance && ascending rest
      in
      check bool
        (Printf.sprintf "dim=%d ascending" dim)
        true (ascending results))
    [ 8; 32; 64; 128 ]

let test_large_index_with_deletion () =
  with_temp_db "hnsw_large" @@ fun db ->
  let dim = 16 in
  let n = 500 in
  let vids =
    with_txn db (fun txn ->
        Array.init n (fun _ ->
            let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
            ok_exn
              (Gvecdb.create_vector db ~txn Node node "e"
                 (floats_to_bigstring (random_vector dim)))))
  in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:20 ~ef:100 ~vector_tag:"e"
         (random_vector dim))
  in
  check int "got 20" 20 (List.length results);
  let rec ascending = function
    | [] | [ _ ] -> true
    | a :: (b :: _ as rest) -> a.Gvecdb.distance <= b.distance && ascending rest
  in
  check bool "ascending" true (ascending results);
  with_txn db (fun txn ->
      for i = 0 to 374 do
        ok_exn (Gvecdb.delete_vector db ~txn vids.(i))
      done);
  let after =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:125 ~ef:200 ~vector_tag:"e"
         (random_vector dim))
  in
  check bool "got most remaining" true (List.length after >= 100);
  List.iter
    (fun r ->
      let remaining = Array.sub vids 375 125 in
      check bool "from remaining set" true
        (Array.exists (fun id -> id = r.Gvecdb.vector_id) remaining))
    after

let test_dimension_mismatch_query () =
  with_temp_db "dim_mismatch" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  with_txn db (fun txn ->
      let _ =
        ok_exn
          (Gvecdb.create_vector db ~txn Node n1 "e"
             (floats_to_bigstring [| 1.0; 0.0; 0.0; 0.0 |]))
      in
      ());
  match
    Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:50 ~vector_tag:"e"
      [| 1.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0 |]
  with
  | Error (Gvecdb.Storage_error _) -> ()
  | _ -> fail "expected dimension mismatch error"

let () =
  run "HNSW"
    [
      ( "basic",
        [
          ("basic_search", `Quick, test_hnsw_basic);
          ("per_tag_isolation", `Quick, test_per_tag_isolation);
          ("soft_delete", `Quick, test_soft_delete);
        ] );
      ( "quality",
        [
          ("recall", `Slow, test_recall);
          ("ef_sweep", `Slow, test_ef_sweep);
          ("dimension_sweep", `Slow, test_dimension_sweep);
        ] );
      ( "persistence",
        [
          ("rebuild_index", `Quick, test_rebuild_index);
          ("persistence_roundtrip", `Quick, test_persistence);
          ("entry_point_deletion", `Quick, test_entry_point_deletion);
        ] );
      ( "stress",
        [
          ("large_index_with_deletion", `Slow, test_large_index_with_deletion);
          ("dimension_mismatch_query", `Quick, test_dimension_mismatch_query);
        ] );
    ]
