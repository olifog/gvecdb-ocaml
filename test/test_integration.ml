open Alcotest
module Bigstring = Bigstringaf

let floats_to_bigstring (arr : float array) : Gvecdb.bigstring =
  let n = Array.length arr in
  let bs = Bigstring.create (n * 4) in
  for i = 0 to n - 1 do
    Bigstring.set_int32_le bs (i * 4) (Int32.bits_of_float arr.(i))
  done;
  bs

let random_vector dim = Array.init dim (fun _ -> Random.float 2.0 -. 1.0)

let ok_exn = function
  | Ok x -> x
  | Error e -> Alcotest.fail (Gvecdb.Error.to_string e)

let with_txn db f =
  match Gvecdb.with_transaction db f with
  | Some x -> x
  | None -> Alcotest.fail "transaction aborted unexpectedly"

let get_temp_path prefix =
  Filename.(
    concat (get_temp_dir_name ())
      (Printf.sprintf "%s_%d_%d.db" prefix (Unix.getpid ()) (Random.int 100000)))

let cleanup_db_files path =
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

let with_temp_db prefix f =
  let path = get_temp_path prefix in
  cleanup_db_files path;
  let db = ok_exn (Gvecdb.create path) in
  Fun.protect
    ~finally:(fun () ->
      Gvecdb.close db;
      cleanup_db_files path)
    (fun () -> f db)

let test_full_graph_lifecycle () =
  with_temp_db "lifecycle" @@ fun db ->
  let dim = 64 in
  let doc_nodes =
    Array.init 50 (fun i ->
        let node = ok_exn (Gvecdb.create_node db "document") in
        let n_vecs = 1 + (i mod 3) in
        with_txn db (fun txn ->
            for _ = 1 to n_vecs do
              let _ =
                ok_exn
                  (Gvecdb.create_vector db ~txn Node node "embedding"
                     (floats_to_bigstring (random_vector dim)))
              in
              ()
            done);
        node)
  in
  with_txn db (fun txn ->
      for i = 0 to 99 do
        let src = doc_nodes.(i mod 50) in
        let dst = doc_nodes.(((i * 7) + 13) mod 50) in
        let _ =
          ok_exn
            (Gvecdb.create_edge db ~txn
               (if i mod 2 = 0 then "cites" else "similar_to")
               src dst)
        in
        ()
      done);
  let query = random_vector dim in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:100 ~vector_tag:"embedding"
         query)
  in
  check bool "got results" true (List.length results > 0);
  for i = 0 to 24 do
    ok_exn (Gvecdb.delete_node db doc_nodes.(i))
  done;
  for i = 25 to 49 do
    check bool
      (Printf.sprintf "node %d exists" i)
      true
      (ok_exn (Gvecdb.node_exists db doc_nodes.(i)))
  done;
  let results2 =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:100 ~vector_tag:"embedding"
         query)
  in
  check bool "still got results" true (List.length results2 > 0);
  with_txn db (fun txn ->
      ok_exn (Gvecdb.rebuild_hnsw_index db ~txn ~vector_tag:"embedding" ()));
  let results3 =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:100 ~vector_tag:"embedding"
         query)
  in
  check bool "search after rebuild" true (List.length results3 > 0)

let test_multiple_restart_cycles () =
  let path = get_temp_path "restart" in
  cleanup_db_files path;
  let dim = 16 in
  let expected_nodes = ref [] in
  let db = ok_exn (Gvecdb.create path) in
  for i = 0 to 9 do
    let n = ok_exn (Gvecdb.create_node db "doc") in
    expected_nodes := (n, i) :: !expected_nodes;
    with_txn db (fun txn ->
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n "emb"
               (floats_to_bigstring
                  (Array.init dim (fun j -> float_of_int ((i * 100) + j)))))
        in
        ())
  done;
  Gvecdb.close db;
  let db = ok_exn (Gvecdb.create path) in
  List.iter
    (fun (n, _) ->
      check bool "exists after restart" true (ok_exn (Gvecdb.node_exists db n)))
    !expected_nodes;
  let to_delete = List.filter (fun (_, i) -> i mod 3 = 0) !expected_nodes in
  List.iter (fun (n, _) -> ok_exn (Gvecdb.delete_node db n)) to_delete;
  expected_nodes := List.filter (fun (_, i) -> i mod 3 <> 0) !expected_nodes;
  Gvecdb.close db;
  let db = ok_exn (Gvecdb.create path) in
  let found =
    List.fold_left
      (fun acc (n, _) ->
        if ok_exn (Gvecdb.node_exists db n) then acc + 1 else acc)
      0 !expected_nodes
  in
  check int "correct count" (List.length !expected_nodes) found;
  let query = Array.init dim (fun i -> float_of_int ((5 * 100) + i)) in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:5 ~ef:50 ~vector_tag:"emb" query)
  in
  check bool "search works" true (List.length results > 0);
  Gvecdb.close db;
  cleanup_db_files path

let test_large_persistence_recall () =
  let path = get_temp_path "large_persist" in
  cleanup_db_files path;
  let dim = 64 in
  let db = ok_exn (Gvecdb.create path) in
  with_txn db (fun txn ->
      for _ = 0 to 499 do
        let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n "emb"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);
  let queries = Array.init 10 (fun _ -> random_vector dim) in
  let before =
    Array.map
      (fun q ->
        ok_exn
          (Gvecdb.knn_hnsw db ~metric:Cosine ~k:20 ~ef:100 ~vector_tag:"emb" q))
      queries
  in
  Gvecdb.close db;
  let db = ok_exn (Gvecdb.create path) in
  let after =
    Array.map
      (fun q ->
        ok_exn
          (Gvecdb.knn_hnsw db ~metric:Cosine ~k:20 ~ef:100 ~vector_tag:"emb" q))
      queries
  in
  for i = 0 to 9 do
    let before_ids = List.map (fun r -> r.Gvecdb.vector_id) before.(i) in
    let after_ids = List.map (fun r -> r.Gvecdb.vector_id) after.(i) in
    let overlap =
      List.fold_left
        (fun acc id -> if List.mem id before_ids then acc + 1 else acc)
        0 after_ids
    in
    let pct =
      float_of_int overlap /. float_of_int (max 1 (List.length before_ids))
    in
    check bool (Printf.sprintf "query %d >= 80%% overlap" i) true (pct >= 0.8)
  done;
  Gvecdb.close db;
  cleanup_db_files path

let test_graph_structure_persistence () =
  let path = get_temp_path "graph_persist" in
  cleanup_db_files path;
  let db = ok_exn (Gvecdb.create path) in
  let nodes = Array.init 20 (fun _ -> ok_exn (Gvecdb.create_node db "node")) in
  with_txn db (fun txn ->
      Array.iteri
        (fun i src ->
          let _ =
            ok_exn
              (Gvecdb.create_edge db ~txn "next" src nodes.((i + 1) mod 20))
          in
          let _ =
            ok_exn
              (Gvecdb.create_edge db ~txn "skip" src nodes.((i + 3) mod 20))
          in
          ())
        nodes);
  Gvecdb.close db;
  let db = ok_exn (Gvecdb.create path) in
  Array.iteri
    (fun i node ->
      let out = ok_exn (Gvecdb.get_outbound_edges db node ()) in
      check int (Printf.sprintf "node %d out" i) 2 (List.length out);
      let types = List.map (fun e -> e.Gvecdb.edge_type) out in
      check bool "has next" true (List.mem "next" types);
      check bool "has skip" true (List.mem "skip" types);
      let inb = ok_exn (Gvecdb.get_inbound_edges db node ()) in
      check int (Printf.sprintf "node %d in" i) 2 (List.length inb))
    nodes;
  Gvecdb.close db;
  cleanup_db_files path

let test_concurrent_readers_single_writer () =
  with_temp_db "concurrent_rw" @@ fun db ->
  let dim = 32 in
  with_txn db (fun txn ->
      for _ = 0 to 99 do
        let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n "emb"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);
  let errors = ref [] in
  let read_count = ref 0 in
  let write_count = ref 0 in
  let running = ref true in
  let mutex = Mutex.create () in
  let readers =
    Array.init 4 (fun id ->
        Thread.create
          (fun () ->
            while !running do
              try
                let _ =
                  ok_exn
                    (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50
                       ~vector_tag:"emb" (random_vector dim))
                in
                Mutex.lock mutex;
                incr read_count;
                Mutex.unlock mutex
              with e ->
                Mutex.lock mutex;
                errors :=
                  Printf.sprintf "reader %d: %s" id (Printexc.to_string e)
                  :: !errors;
                Mutex.unlock mutex
            done)
          ())
  in
  let writer =
    Thread.create
      (fun () ->
        for _ = 0 to 49 do
          try
            with_txn db (fun txn ->
                let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
                let _ =
                  ok_exn
                    (Gvecdb.create_vector db ~txn Node n "emb"
                       (floats_to_bigstring (random_vector dim)))
                in
                ());
            Mutex.lock mutex;
            incr write_count;
            Mutex.unlock mutex;
            Thread.delay 0.001
          with e ->
            Mutex.lock mutex;
            errors :=
              Printf.sprintf "writer: %s" (Printexc.to_string e) :: !errors;
            Mutex.unlock mutex
        done;
        running := false)
      ()
  in
  Thread.join writer;
  Array.iter Thread.join readers;
  check (list string) "no errors" [] !errors;
  check bool "writes completed" true (!write_count >= 40);
  check bool "reads completed" true (!read_count > 10)

let test_hnsw_search_during_modifications () =
  with_temp_db "hnsw_concurrent" @@ fun db ->
  let dim = 32 in
  with_txn db (fun txn ->
      for _ = 0 to 199 do
        let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n "emb"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);
  let search_count = ref 0 in
  let errors = ref [] in
  let running = ref true in
  let mutex = Mutex.create () in
  let searcher =
    Thread.create
      (fun () ->
        while !running do
          try
            let _ =
              ok_exn
                (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50
                   ~vector_tag:"emb" (random_vector dim))
            in
            Mutex.lock mutex;
            incr search_count;
            Mutex.unlock mutex
          with e ->
            Mutex.lock mutex;
            errors :=
              Printf.sprintf "searcher: %s" (Printexc.to_string e) :: !errors;
            Mutex.unlock mutex
        done)
      ()
  in
  let modifier =
    Thread.create
      (fun () ->
        let created = ref [] in
        for i = 0 to 49 do
          (try
             if i mod 2 = 0 then
               let n = ok_exn (Gvecdb.create_node db "doc") in
               let v =
                 with_txn db (fun txn ->
                     ok_exn
                       (Gvecdb.create_vector db ~txn Node n "emb"
                          (floats_to_bigstring (random_vector dim))))
               in
               created := v :: !created
             else
               match !created with
               | v :: rest ->
                   with_txn db (fun txn ->
                       try ok_exn (Gvecdb.delete_vector db ~txn v)
                       with _ -> ());
                   created := rest
               | [] -> ()
           with _ -> ());
          Thread.delay 0.002
        done;
        running := false)
      ()
  in
  Thread.join modifier;
  Thread.join searcher;
  check (list string) "no errors" [] !errors;
  check bool "searches completed" true (!search_count > 10)

let test_high_dimensional () =
  with_temp_db "high_dim" @@ fun db ->
  let dim = 1536 in
  with_txn db (fun txn ->
      for _ = 0 to 99 do
        let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n "emb"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"emb"
         (random_vector dim))
  in
  check int "got results" 10 (List.length results)

let () =
  run "Integration"
    [
      ( "lifecycle",
        [ ("full_graph_lifecycle", `Slow, test_full_graph_lifecycle) ] );
      ( "persistence",
        [
          ("multiple_restart_cycles", `Slow, test_multiple_restart_cycles);
          ("large_persistence_recall", `Slow, test_large_persistence_recall);
          ( "graph_structure_persistence",
            `Quick,
            test_graph_structure_persistence );
        ] );
      ( "concurrency",
        [
          ( "concurrent_readers_single_writer",
            `Slow,
            test_concurrent_readers_single_writer );
          ( "hnsw_search_during_modifications",
            `Slow,
            test_hnsw_search_during_modifications );
        ] );
      ( "edge_cases",
        [ ("high_dimensional_vectors", `Slow, test_high_dimensional) ] );
    ]
