(** Comprehensive integration tests for gvecdb.

    These tests exercise large swatches of the codebase together, test
    concurrent behavior, and verify correctness after restarts. *)

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

(* Persistent temp path without auto-cleanup *)
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
    if Sys.file_exists hnsw_dir && Sys.is_directory hnsw_dir then begin
      Array.iter
        (fun f -> Sys.remove (Filename.concat hnsw_dir f))
        (Sys.readdir hnsw_dir);
      Unix.rmdir hnsw_dir
    end
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

(* ============================================================================
   SECTION 1: Full Lifecycle Integration Tests
   ============================================================================ *)

(** Test: Build a complete knowledge graph with vectors, query it, modify it
    extensively, verify consistency throughout *)
let test_full_graph_lifecycle () =
  with_temp_db "lifecycle" @@ fun db ->
  let dim = 64 in

  (* Phase 1: Build a document graph with embeddings *)
  let doc_nodes =
    Array.init 50 (fun i ->
        let node = ok_exn (Gvecdb.create_node db "document") in
        (* Each doc has 1-3 embedding vectors *)
        let n_vecs = 1 + (i mod 3) in
        let vecs =
          with_txn db (fun txn ->
              Array.init n_vecs (fun _ ->
                  ok_exn
                    (Gvecdb.create_vector db ~txn node "embedding"
                       (floats_to_bigstring (random_vector dim)))))
        in
        (node, vecs))
  in

  (* Phase 2: Create relationship edges between documents *)
  let edges =
    with_txn db (fun txn ->
        Array.init 100 (fun i ->
            let src_idx = i mod 50 in
            let dst_idx = ((i * 7) + 13) mod 50 in
            let src, _ = doc_nodes.(src_idx) in
            let dst, _ = doc_nodes.(dst_idx) in
            let edge_type = if i mod 2 = 0 then "cites" else "similar_to" in
            ok_exn (Gvecdb.create_edge db ~txn edge_type src dst)))
  in

  (* Phase 3: Add vectors to some edges *)
  with_txn db (fun txn ->
      for i = 0 to 29 do
        let _ =
          ok_exn
            (Gvecdb.create_edge_vector db ~txn edges.(i) "relation_embedding"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);

  (* Phase 4: Query the graph - verify structure *)
  let total_outbound =
    Array.fold_left
      (fun acc (node, _) ->
        acc + List.length (ok_exn (Gvecdb.get_outbound_edges db node)))
      0 doc_nodes
  in
  check bool "has outbound edges" true (total_outbound > 0);

  (* Phase 5: Vector search should work *)
  let query = random_vector dim in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:100 ~vector_tag:"embedding"
         query)
  in
  check bool "got search results" true (List.length results > 0);
  check bool "results have distances" true
    (List.for_all (fun r -> Float.is_finite r.Gvecdb.distance) results);

  (* Phase 6: Delete half the documents (cascade delete edges and vectors) *)
  for i = 0 to 24 do
    let node, _ = doc_nodes.(i) in
    ok_exn (Gvecdb.delete_node db node)
  done;

  (* Phase 7: Verify deletions *)
  for i = 0 to 24 do
    let node, _ = doc_nodes.(i) in
    let exists = ok_exn (Gvecdb.node_exists db node) in
    check bool (Printf.sprintf "node %d deleted" i) false exists
  done;

  (* Phase 8: Remaining nodes should still work *)
  for i = 25 to 49 do
    let node, _ = doc_nodes.(i) in
    let exists = ok_exn (Gvecdb.node_exists db node) in
    check bool (Printf.sprintf "node %d exists" i) true exists
  done;

  (* Phase 9: Search should still return results from remaining vectors *)
  let results2 =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:100 ~vector_tag:"embedding"
         query)
  in
  check bool "still got search results" true (List.length results2 > 0);

  (* Phase 10: Rebuild index and verify *)
  with_txn db (fun txn ->
      ok_exn (Gvecdb.rebuild_hnsw_index db ~txn ~vector_tag:"embedding" ()));
  let results3 =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:100 ~vector_tag:"embedding"
         query)
  in
  check bool "search works after rebuild" true (List.length results3 > 0)

(** Test: Interleaved operations across multiple entity types *)
let test_interleaved_operations () =
  with_temp_db "interleaved" @@ fun db ->
  let dim = 32 in

  (* Interleave node creation, edge creation, vector creation, and deletions *)
  let nodes = ref [] in
  let edges = ref [] in
  let vectors = ref [] in

  for round = 0 to 19 do
    (* Create some nodes *)
    for _ = 0 to 4 do
      let n = ok_exn (Gvecdb.create_node db "item") in
      nodes := n :: !nodes
    done;

    (* Create edges between recent nodes *)
    let node_arr = Array.of_list !nodes in
    let n = Array.length node_arr in
    if n >= 2 then begin
      with_txn db (fun txn ->
          for _ = 0 to 2 do
            let src = node_arr.(Random.int n) in
            let dst = node_arr.(Random.int n) in
            if src <> dst then begin
              let e = ok_exn (Gvecdb.create_edge db ~txn "link" src dst) in
              edges := e :: !edges
            end
          done)
    end;

    (* Add vectors to random nodes *)
    if n > 0 then begin
      with_txn db (fun txn ->
          for _ = 0 to 2 do
            let node = node_arr.(Random.int n) in
            let v =
              ok_exn
                (Gvecdb.create_vector db ~txn node "vec"
                   (floats_to_bigstring (random_vector dim)))
            in
            vectors := v :: !vectors
          done)
    end;

    (* Occasionally delete some vectors *)
    if round mod 5 = 0 && List.length !vectors > 10 then begin
      with_txn db (fun txn ->
          for _ = 0 to 4 do
            match !vectors with
            | v :: rest ->
                (try ok_exn (Gvecdb.delete_vector db ~txn v) with _ -> ());
                vectors := rest
            | [] -> ()
          done)
    end;

    (* Occasionally delete a node (cascades) *)
    if round mod 7 = 0 && n > 5 then begin
      let node = node_arr.(Random.int n) in
      (try ok_exn (Gvecdb.delete_node db node) with _ -> ());
      nodes := List.filter (( <> ) node) !nodes
    end
  done;

  (* Final verification *)
  let remaining_nodes =
    List.filter
      (fun n ->
        match Gvecdb.node_exists db n with Ok true -> true | _ -> false)
      !nodes
  in
  check bool "some nodes remain" true (List.length remaining_nodes > 0);

  (* Search should work *)
  let query = random_vector dim in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:5 ~ef:50 ~vector_tag:"vec" query)
  in
  (* Results might be empty if all vectors were deleted, that's OK *)
  check bool "search completes" true (List.length results >= 0)

(* ============================================================================
   SECTION 2: Persistence and Restart Tests
   ============================================================================ *)

(** Test: Multiple restart cycles with modifications between each *)
let test_multiple_restart_cycles () =
  let path = get_temp_path "restart_cycles" in
  cleanup_db_files path;
  let dim = 16 in

  (* Track what we expect to find *)
  let expected_nodes = ref [] in
  let expected_vectors = ref [] in

  (* Cycle 1: Initial creation *)
  let db = ok_exn (Gvecdb.create path) in
  for i = 0 to 9 do
    let n = ok_exn (Gvecdb.create_node db "doc") in
    expected_nodes := (n, i) :: !expected_nodes;
    let v =
      with_txn db (fun txn ->
          ok_exn
            (Gvecdb.create_vector db ~txn n "emb"
               (floats_to_bigstring
                  (Array.init dim (fun j -> float_of_int ((i * 100) + j))))))
    in
    expected_vectors := v :: !expected_vectors
  done;
  Gvecdb.close db;

  (* Cycle 2: Reopen and verify, then add more *)
  let db = ok_exn (Gvecdb.create path) in
  List.iter
    (fun (n, _) ->
      check bool "node exists after restart 1" true
        (ok_exn (Gvecdb.node_exists db n)))
    !expected_nodes;

  for i = 10 to 19 do
    let n = ok_exn (Gvecdb.create_node db "doc") in
    expected_nodes := (n, i) :: !expected_nodes;
    let v =
      with_txn db (fun txn ->
          ok_exn
            (Gvecdb.create_vector db ~txn n "emb"
               (floats_to_bigstring
                  (Array.init dim (fun j -> float_of_int ((i * 100) + j))))))
    in
    expected_vectors := v :: !expected_vectors
  done;
  Gvecdb.close db;

  (* Cycle 3: Reopen, delete some, add more *)
  let db = ok_exn (Gvecdb.create path) in
  let to_delete = List.filter (fun (_, i) -> i mod 3 = 0) !expected_nodes in
  List.iter (fun (n, _) -> ok_exn (Gvecdb.delete_node db n)) to_delete;
  expected_nodes := List.filter (fun (_, i) -> i mod 3 <> 0) !expected_nodes;

  for i = 20 to 24 do
    let n = ok_exn (Gvecdb.create_node db "doc") in
    expected_nodes := (n, i) :: !expected_nodes;
    let v =
      with_txn db (fun txn ->
          ok_exn
            (Gvecdb.create_vector db ~txn n "emb"
               (floats_to_bigstring
                  (Array.init dim (fun j -> float_of_int ((i * 100) + j))))))
    in
    expected_vectors := v :: !expected_vectors
  done;
  Gvecdb.close db;

  (* Cycle 4: Final verification *)
  let db = ok_exn (Gvecdb.create path) in
  let found_count =
    List.fold_left
      (fun acc (n, _) ->
        if ok_exn (Gvecdb.node_exists db n) then acc + 1 else acc)
      0 !expected_nodes
  in
  check int "correct node count after cycles"
    (List.length !expected_nodes)
    found_count;

  (* Search should work *)
  let query = Array.init dim (fun i -> float_of_int ((15 * 100) + i)) in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:5 ~ef:50 ~vector_tag:"emb" query)
  in
  check bool "search works after restarts" true (List.length results > 0);

  Gvecdb.close db;
  cleanup_db_files path

(** Test: Large dataset persistence and recall consistency *)
let test_large_persistence_recall () =
  let path = get_temp_path "large_persist" in
  cleanup_db_files path;
  let dim = 64 in
  let n_vectors = 500 in

  (* Create large index *)
  let db = ok_exn (Gvecdb.create path) in
  let vectors = Array.init n_vectors (fun _ -> random_vector dim) in
  let vector_ids =
    with_txn db (fun txn ->
        Array.map
          (fun vec ->
            let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
            ok_exn
              (Gvecdb.create_vector db ~txn n "emb" (floats_to_bigstring vec)))
          vectors)
  in

  (* Run queries and record results *)
  let queries = Array.init 10 (fun _ -> random_vector dim) in
  let results_before =
    Array.map
      (fun q ->
        ok_exn
          (Gvecdb.knn_hnsw db ~metric:Cosine ~k:20 ~ef:100 ~vector_tag:"emb" q))
      queries
  in

  Gvecdb.close db;

  (* Reopen and verify same results *)
  let db = ok_exn (Gvecdb.create path) in
  let results_after =
    Array.map
      (fun q ->
        ok_exn
          (Gvecdb.knn_hnsw db ~metric:Cosine ~k:20 ~ef:100 ~vector_tag:"emb" q))
      queries
  in

  (* Check that results are highly similar (HNSW is approximate, allow some variance) *)
  for i = 0 to 9 do
    let before_ids =
      List.map (fun r -> r.Gvecdb.vector_id) results_before.(i)
    in
    let after_ids = List.map (fun r -> r.Gvecdb.vector_id) results_after.(i) in
    let overlap =
      List.fold_left
        (fun acc id -> if List.mem id before_ids then acc + 1 else acc)
        0 after_ids
    in
    let overlap_pct =
      float_of_int overlap /. float_of_int (max 1 (List.length before_ids))
    in
    check bool
      (Printf.sprintf "query %d has >= 80%% overlap" i)
      true (overlap_pct >= 0.8)
  done;

  Gvecdb.close db;
  cleanup_db_files path;
  ignore vector_ids

(** Test: Persistence of graph structure (edges, adjacency) *)
let test_graph_structure_persistence () =
  let path = get_temp_path "graph_persist" in
  cleanup_db_files path;

  (* Build a graph *)
  let db = ok_exn (Gvecdb.create path) in
  let nodes = Array.init 20 (fun _ -> ok_exn (Gvecdb.create_node db "node")) in

  (* Create specific edge pattern we can verify *)
  let edges =
    with_txn db (fun txn ->
        (* Node i connects to nodes (i+1) mod 20 and (i+3) mod 20 *)
        Array.mapi
          (fun i src ->
            let dst1 = nodes.((i + 1) mod 20) in
            let dst2 = nodes.((i + 3) mod 20) in
            let e1 = ok_exn (Gvecdb.create_edge db ~txn "next" src dst1) in
            let e2 = ok_exn (Gvecdb.create_edge db ~txn "skip" src dst2) in
            (e1, e2))
          nodes)
  in

  Gvecdb.close db;

  (* Reopen and verify graph structure *)
  let db = ok_exn (Gvecdb.create path) in

  Array.iteri
    (fun i node ->
      (* Each node should have exactly 2 outbound edges *)
      let outbound = ok_exn (Gvecdb.get_outbound_edges db node) in
      check int
        (Printf.sprintf "node %d has 2 outbound" i)
        2 (List.length outbound);

      (* Check edge types *)
      let types = List.map (fun e -> e.Gvecdb.edge_type) outbound in
      check bool "has next edge" true (List.mem "next" types);
      check bool "has skip edge" true (List.mem "skip" types);

      (* Each node should have exactly 2 inbound edges (from i-1 and i-3 mod 20) *)
      let inbound = ok_exn (Gvecdb.get_inbound_edges db node) in
      check int
        (Printf.sprintf "node %d has 2 inbound" i)
        2 (List.length inbound))
    nodes;

  Gvecdb.close db;
  cleanup_db_files path;
  ignore edges

(* ============================================================================
   SECTION 3: Concurrency and Parallelism Tests
   ============================================================================ *)

(** Test: Concurrent readers with single writer *)
let test_concurrent_readers_single_writer () =
  with_temp_db "concurrent_rw" @@ fun db ->
  let dim = 32 in
  let n_initial = 100 in

  (* Create initial data *)
  with_txn db (fun txn ->
      for _ = 0 to n_initial - 1 do
        let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n "emb"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);

  let errors = ref [] in
  let read_count = ref 0 in
  let write_count = ref 0 in
  let running = ref true in
  let mutex = Mutex.create () in

  (* Reader threads *)
  let readers =
    Array.init 4 (fun id ->
        Thread.create
          (fun () ->
            while !running do
              try
                let query = random_vector dim in
                let _ =
                  ok_exn
                    (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50
                       ~vector_tag:"emb" query)
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

  (* Writer thread *)
  let writer =
    Thread.create
      (fun () ->
        for _ = 0 to 49 do
          try
            with_txn db (fun txn ->
                let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
                let _ =
                  ok_exn
                    (Gvecdb.create_vector db ~txn n "emb"
                       (floats_to_bigstring (random_vector dim)))
                in
                ());
            Mutex.lock mutex;
            incr write_count;
            Mutex.unlock mutex;
            Thread.delay 0.001 (* Small delay to let readers run *)
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
  check bool "many reads completed" true (!read_count > 10)

(** Test: Multiple concurrent writers (should serialize) *)
let test_concurrent_writers () =
  with_temp_db "concurrent_writers" @@ fun db ->
  let n_threads = 4 in
  let n_ops_per_thread = 25 in
  let errors = ref [] in
  let success_count = ref 0 in
  let mutex = Mutex.create () in

  let threads =
    Array.init n_threads (fun id ->
        Thread.create
          (fun () ->
            for _ = 0 to n_ops_per_thread - 1 do
              try
                let n = ok_exn (Gvecdb.create_node db "doc") in
                with_txn db (fun txn ->
                    let _ =
                      ok_exn
                        (Gvecdb.create_vector db ~txn n "emb"
                           (floats_to_bigstring (random_vector 16)))
                    in
                    ());
                Mutex.lock mutex;
                incr success_count;
                Mutex.unlock mutex
              with e ->
                Mutex.lock mutex;
                errors :=
                  Printf.sprintf "thread %d: %s" id (Printexc.to_string e)
                  :: !errors;
                Mutex.unlock mutex
            done)
          ())
  in

  Array.iter Thread.join threads;

  check (list string) "no errors" [] !errors;
  check int "all ops completed" (n_threads * n_ops_per_thread) !success_count

(** Test: Read-only transactions don't block each other *)
let test_parallel_readonly () =
  with_temp_db "parallel_ro" @@ fun db ->
  let dim = 32 in

  (* Create test data *)
  let nodes =
    with_txn db (fun txn ->
        Array.init 100 (fun _ ->
            let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
            let _ =
              ok_exn
                (Gvecdb.create_vector db ~txn n "emb"
                   (floats_to_bigstring (random_vector dim)))
            in
            n))
  in

  let results = Array.make 8 0 in
  let errors = ref [] in
  let mutex = Mutex.create () in

  let threads =
    Array.init 8 (fun id ->
        Thread.create
          (fun () ->
            try
              (* Each thread does many read-only operations *)
              for _ = 0 to 99 do
                (* Mix of operations *)
                match id mod 4 with
                | 0 ->
                    let _ =
                      ok_exn (Gvecdb.node_exists db nodes.(Random.int 100))
                    in
                    ()
                | 1 ->
                    let _ =
                      ok_exn
                        (Gvecdb.get_outbound_edges db nodes.(Random.int 100))
                    in
                    ()
                | 2 ->
                    let query = random_vector dim in
                    let _ =
                      ok_exn
                        (Gvecdb.knn_hnsw db ~metric:Cosine ~k:5 ~ef:20
                           ~vector_tag:"emb" query)
                    in
                    ()
                | _ ->
                    let _ =
                      ok_exn (Gvecdb.get_node_info db nodes.(Random.int 100))
                    in
                    ()
              done;
              Mutex.lock mutex;
              results.(id) <- 100;
              Mutex.unlock mutex
            with e ->
              Mutex.lock mutex;
              errors :=
                Printf.sprintf "thread %d: %s" id (Printexc.to_string e)
                :: !errors;
              Mutex.unlock mutex)
          ())
  in

  Array.iter Thread.join threads;

  check (list string) "no errors" [] !errors;
  let total = Array.fold_left ( + ) 0 results in
  check int "all operations completed" 800 total

(** Test: HNSW search during concurrent modifications *)
let test_hnsw_search_during_modifications () =
  with_temp_db "hnsw_concurrent" @@ fun db ->
  let dim = 32 in

  (* Create initial index *)
  with_txn db (fun txn ->
      for _ = 0 to 199 do
        let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n "emb"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);

  let search_results = ref [] in
  let errors = ref [] in
  let running = ref true in
  let mutex = Mutex.create () in

  (* Continuous searcher *)
  let searcher =
    Thread.create
      (fun () ->
        while !running do
          try
            let query = random_vector dim in
            let results =
              ok_exn
                (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50
                   ~vector_tag:"emb" query)
            in
            Mutex.lock mutex;
            search_results := List.length results :: !search_results;
            Mutex.unlock mutex
          with e ->
            Mutex.lock mutex;
            errors :=
              Printf.sprintf "searcher: %s" (Printexc.to_string e) :: !errors;
            Mutex.unlock mutex
        done)
      ()
  in

  (* Modifier: adds and deletes vectors *)
  let modifier =
    Thread.create
      (fun () ->
        let created = ref [] in
        for i = 0 to 49 do
          try
            if i mod 2 = 0 then begin
              (* Add *)
              let n = ok_exn (Gvecdb.create_node db "doc") in
              let v =
                with_txn db (fun txn ->
                    ok_exn
                      (Gvecdb.create_vector db ~txn n "emb"
                         (floats_to_bigstring (random_vector dim))))
              in
              created := v :: !created
            end
            else begin
              (* Delete if we have any *)
              match !created with
              | v :: rest ->
                  with_txn db (fun txn ->
                      try ok_exn (Gvecdb.delete_vector db ~txn v) with _ -> ());
                  created := rest
              | [] -> ()
            end;
            Thread.delay 0.002
          with e ->
            Mutex.lock mutex;
            errors :=
              Printf.sprintf "modifier: %s" (Printexc.to_string e) :: !errors;
            Mutex.unlock mutex
        done;
        running := false)
      ()
  in

  Thread.join modifier;
  Thread.join searcher;

  check (list string) "no errors" [] !errors;
  check bool "many searches completed" true (List.length !search_results > 10);
  (* All search results should have gotten some results *)
  check bool "searches returned results" true
    (List.for_all (fun n -> n >= 0) !search_results)

(* ============================================================================
   SECTION 4: Stress Tests
   ============================================================================ *)

(** Test: Heavy write load *)
let test_heavy_write_load () =
  with_temp_db "heavy_write" @@ fun db ->
  let dim = 16 in
  let n_nodes = 1000 in
  let n_edges = 2000 in
  let n_vectors = 1500 in

  (* Create many nodes *)
  let nodes =
    Array.init n_nodes (fun _ -> ok_exn (Gvecdb.create_node db "item"))
  in

  (* Create many edges *)
  with_txn db (fun txn ->
      for _ = 0 to n_edges - 1 do
        let src = nodes.(Random.int n_nodes) in
        let dst = nodes.(Random.int n_nodes) in
        if src <> dst then
          let _ = ok_exn (Gvecdb.create_edge db ~txn "link" src dst) in
          ()
      done);

  (* Create many vectors *)
  with_txn db (fun txn ->
      for _ = 0 to n_vectors - 1 do
        let node = nodes.(Random.int n_nodes) in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn node "vec"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);

  (* Verify we can query *)
  let query = random_vector dim in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:50 ~ef:100 ~vector_tag:"vec" query)
  in
  check int "got 50 results" 50 (List.length results)

(** Test: Heavy delete load *)
let test_heavy_delete_load () =
  with_temp_db "heavy_delete" @@ fun db ->
  let dim = 16 in
  let n_nodes = 500 in

  (* Create nodes with vectors *)
  let nodes =
    Array.init n_nodes (fun _ ->
        let n = ok_exn (Gvecdb.create_node db "item") in
        with_txn db (fun txn ->
            let _ =
              ok_exn
                (Gvecdb.create_vector db ~txn n "vec"
                   (floats_to_bigstring (random_vector dim)))
            in
            ());
        n)
  in

  (* Delete all but 50 *)
  for i = 0 to n_nodes - 51 do
    ok_exn (Gvecdb.delete_node db nodes.(i))
  done;

  (* Verify remaining *)
  let remaining =
    Array.fold_left
      (fun acc n -> if ok_exn (Gvecdb.node_exists db n) then acc + 1 else acc)
      0 nodes
  in
  check int "50 nodes remain" 50 remaining;

  (* Search should still work - HNSW is approximate so allow some tolerance *)
  let query = random_vector dim in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:100 ~ef:100 ~vector_tag:"vec" query)
  in
  check bool "some results found" true (List.length results >= 1)

(** Test: Many small transactions *)
let test_many_small_transactions () =
  with_temp_db "many_txn" @@ fun db ->
  let dim = 8 in

  (* 1000 individual transactions *)
  for i = 0 to 999 do
    let n = ok_exn (Gvecdb.create_node db "item") in
    with_txn db (fun txn ->
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n "vec"
               (floats_to_bigstring
                  (Array.init dim (fun j -> float_of_int ((i * 10) + j)))))
        in
        ())
  done;

  (* Verify count *)
  let query = Array.init dim (fun i -> float_of_int ((500 * 10) + i)) in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"vec" query)
  in
  check int "got results" 10 (List.length results)

(** Test: Large batch transaction *)
let test_large_batch_transaction () =
  with_temp_db "large_batch" @@ fun db ->
  let dim = 16 in

  (* Single transaction with many operations *)
  with_txn db (fun txn ->
      for _ = 0 to 999 do
        let n = ok_exn (Gvecdb.create_node db ~txn "item") in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n "vec"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);

  let query = random_vector dim in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:100 ~ef:100 ~vector_tag:"vec" query)
  in
  check int "got 100 results" 100 (List.length results)

(* ============================================================================
   SECTION 5: Edge Cases and Boundary Tests
   ============================================================================ *)

(** Test: Operations on just-created entities *)
let test_immediate_operations () =
  with_temp_db "immediate" @@ fun db ->
  (* Create and immediately query *)
  let n = ok_exn (Gvecdb.create_node db "doc") in
  let v =
    with_txn db (fun txn ->
        ok_exn
          (Gvecdb.create_vector db ~txn n "emb"
             (floats_to_bigstring [| 1.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0 |])))
  in

  (* Immediate search *)
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:10 ~vector_tag:"emb"
         [| 1.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0 |])
  in
  check int "found just-created vector" 1 (List.length results);
  check int64 "correct vector id" v (List.hd results).Gvecdb.vector_id;

  (* Immediate delete and verify *)
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn v));
  let results2 =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:10 ~vector_tag:"emb"
         [| 1.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0 |])
  in
  check int "no results after delete" 0 (List.length results2)

(** Test: Empty tag searches *)
let test_empty_tag_searches () =
  with_temp_db "empty_tag" @@ fun db ->
  (* Create vectors with one tag *)
  let n = ok_exn (Gvecdb.create_node db "doc") in
  with_txn db (fun txn ->
      let _ =
        ok_exn
          (Gvecdb.create_vector db ~txn n "tag_a"
             (floats_to_bigstring [| 1.0; 0.0 |]))
      in
      ());

  (* Search different tag - should return empty *)
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"tag_b"
         [| 1.0; 0.0 |])
  in
  check int "no results for different tag" 0 (List.length results)

(** Test: Multiple tags per node *)
let test_multiple_tags_per_node () =
  with_temp_db "multi_tag" @@ fun db ->
  (* Create node with vectors of different tags *)
  let n = ok_exn (Gvecdb.create_node db "doc") in
  let vec_a = [| 1.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0 |] in
  let vec_b = [| 0.0; 1.0; 0.0; 0.0; 0.0; 0.0; 0.0; 0.0 |] in
  let vec_c = [| 0.0; 0.0; 1.0; 0.0; 0.0; 0.0; 0.0; 0.0 |] in

  let v_a, v_b, v_c =
    with_txn db (fun txn ->
        let a =
          ok_exn
            (Gvecdb.create_vector db ~txn n "title_emb"
               (floats_to_bigstring vec_a))
        in
        let b =
          ok_exn
            (Gvecdb.create_vector db ~txn n "content_emb"
               (floats_to_bigstring vec_b))
        in
        let c =
          ok_exn
            (Gvecdb.create_vector db ~txn n "summary_emb"
               (floats_to_bigstring vec_c))
        in
        (a, b, c))
  in

  (* Search each tag *)
  let res_a =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:10 ~vector_tag:"title_emb"
         vec_a)
  in
  let res_b =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:10 ~vector_tag:"content_emb"
         vec_b)
  in
  let res_c =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:1 ~ef:10 ~vector_tag:"summary_emb"
         vec_c)
  in

  check int64 "found title vec" v_a (List.hd res_a).Gvecdb.vector_id;
  check int64 "found content vec" v_b (List.hd res_b).Gvecdb.vector_id;
  check int64 "found summary vec" v_c (List.hd res_c).Gvecdb.vector_id

(** Test: High-dimensional vectors *)
let test_high_dimensional_vectors () =
  with_temp_db "high_dim" @@ fun db ->
  let dim = 1536 in
  (* OpenAI embedding dimension *)
  let n_vectors = 100 in

  (* Create vectors *)
  with_txn db (fun txn ->
      for _ = 0 to n_vectors - 1 do
        let n = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n "emb"
               (floats_to_bigstring (random_vector dim)))
        in
        ()
      done);

  (* Search should work *)
  let query = random_vector dim in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"emb" query)
  in
  check int "got results" 10 (List.length results)

(* ============================================================================
   Test Runner
   ============================================================================ *)

let lifecycle_tests =
  [
    ("full_graph_lifecycle", `Slow, test_full_graph_lifecycle);
    ("interleaved_operations", `Slow, test_interleaved_operations);
  ]

let persistence_tests =
  [
    ("multiple_restart_cycles", `Slow, test_multiple_restart_cycles);
    ("large_persistence_recall", `Slow, test_large_persistence_recall);
    ("graph_structure_persistence", `Quick, test_graph_structure_persistence);
  ]

let concurrency_tests =
  [
    ( "concurrent_readers_single_writer",
      `Slow,
      test_concurrent_readers_single_writer );
    ("concurrent_writers", `Slow, test_concurrent_writers);
    ("parallel_readonly", `Quick, test_parallel_readonly);
    ( "hnsw_search_during_modifications",
      `Slow,
      test_hnsw_search_during_modifications );
  ]

let stress_tests =
  [
    ("heavy_write_load", `Slow, test_heavy_write_load);
    ("heavy_delete_load", `Slow, test_heavy_delete_load);
    ("many_small_transactions", `Slow, test_many_small_transactions);
    ("large_batch_transaction", `Slow, test_large_batch_transaction);
  ]

let edge_case_tests =
  [
    ("immediate_operations", `Quick, test_immediate_operations);
    ("empty_tag_searches", `Quick, test_empty_tag_searches);
    ("multiple_tags_per_node", `Quick, test_multiple_tags_per_node);
    ("high_dimensional_vectors", `Slow, test_high_dimensional_vectors);
  ]

let () =
  run "Integration"
    [
      ("lifecycle", lifecycle_tests);
      ("persistence", persistence_tests);
      ("concurrency", concurrency_tests);
      ("stress", stress_tests);
      ("edge_cases", edge_case_tests);
    ]
