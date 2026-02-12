(** ANN Recall & Throughput Benchmark

    Produces recall@k vs QPS data — the standard benchmark for
    approximate nearest neighbor algorithms. *)

open Bench_common

let default_n = 10000
let default_dim = 128
let default_k = 10
let default_n_queries = 100
let default_seed = 42
let query_seed = 12345
let ef_values = [| 10; 20; 50; 100; 200; 400 |]

(** Build an index: insert all vectors into a fresh DB.
    Returns (db, path, build_time_s). *)
let build_index ~vectors ~dim ~metric ~prefix =
  let n = Array.length vectors in
  let path = temp_db_path prefix in
  cleanup_db_files path;
  let db = ok_exn (Gvecdb.create path) in
  let t0 = Unix.gettimeofday () in
  (* Insert in batches of 100 for efficiency *)
  let batch_size = 100 in
  let i = ref 0 in
  while !i < n do
    let batch_end = min n (!i + batch_size) in
    with_txn db (fun txn ->
      for j = !i to batch_end - 1 do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        ignore (ok_exn (Gvecdb.create_vector db ~txn ~metric node "v"
          (floats_to_bigstring vectors.(j))));
        progress ~label:(Printf.sprintf "insert %s %dd" (metric_to_string metric) dim)
          ~i:j ~n
      done);
    i := batch_end
  done;
  let t1 = Unix.gettimeofday () in
  (db, path, t1 -. t0)

(** Compute brute-force ground truth for all queries *)
let compute_ground_truth db queries k metric =
  let n = Array.length queries in
  Array.init n (fun i ->
    progress ~label:"ground truth" ~i ~n;
    let results = ok_exn (Gvecdb.knn_brute_force db ~metric ~k queries.(i)) in
    List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) results)

(** Run ef sweep: for each ef value, run all queries and compute recall + latency *)
let run_ef_sweep db queries ground_truth k metric =
  Array.map (fun ef ->
    Printf.printf "  ef=%d: %!" ef;
    let latencies = Array.mapi (fun i query ->
      let (results, lat_us) = time_us (fun () ->
        ok_exn (Gvecdb.knn_hnsw db ~metric ~k ~ef ~vector_tag:"v" query)) in
      let ids = List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) results in
      let recall = compute_recall ~ground_truth:ground_truth.(i) ~approximate:ids in
      (lat_us, recall)
    ) queries in
    let lats = Array.map fst latencies in
    let recalls = Array.map snd latencies in
    let mean_recall = Array.fold_left ( +. ) 0.0 recalls /. float (Array.length recalls) in
    let stats = compute_stats lats in
    Printf.printf "recall=%.3f qps=%.0f\n%!" mean_recall stats.qps;
    (ef, mean_recall, stats)
  ) ef_values

let run_single_config ~n ~dim ~metric ~k ~n_queries ~seed ~output_dir =
  Printf.printf "\n=== ANN benchmark: n=%d dim=%d metric=%s k=%d ===\n%!"
    n dim (metric_to_string metric) k;

  (* Generate data *)
  let vectors = generate_dataset ~seed ~n ~dim in
  let queries = generate_dataset ~seed:query_seed ~n:n_queries ~dim in

  (* Build index *)
  let (db, path, build_time) = build_index ~vectors ~dim ~metric
    ~prefix:(Printf.sprintf "ann_%d_%d" n dim) in

  Fun.protect ~finally:(fun () -> Gvecdb.close db; cleanup_db_files path) (fun () ->
    let index_size = get_db_size_bytes path in
    Printf.printf "Build: %.2fs (%.0f vec/s), size: %d bytes\n%!"
      build_time (float n /. build_time) index_size;

    (* Ground truth *)
    Gc.compact ();
    let ground_truth = compute_ground_truth db queries k metric in

    (* ef sweep *)
    Gc.compact ();
    let results = run_ef_sweep db queries ground_truth k metric in

    (* Output JSON *)
    let ts = timestamp () in
    let json : Yojson.Basic.t = `Assoc [
      ("benchmark", `String "ann_recall_vs_qps");
      ("timestamp", `String ts);
      ("params", `Assoc [
        ("n", `Int n);
        ("dim", `Int dim);
        ("metric", `String (metric_to_string metric));
        ("k", `Int k);
        ("n_queries", `Int n_queries);
        ("seed", `Int seed);
        ("hnsw_params", hnsw_params_to_json ());
      ]);
      ("build", `Assoc [
        ("time_s", `Float build_time);
        ("vectors_per_second", `Float (float n /. build_time));
        ("index_size_bytes", `Int index_size);
      ]);
      ("results", `List (Array.to_list (Array.map (fun (ef, recall, stats) ->
        `Assoc [
          ("ef", `Int ef);
          ("mean_recall", `Float recall);
          ("qps", `Float stats.qps);
          ("mean_latency_us", `Float stats.mean_us);
          ("p50_latency_us", `Float stats.p50_us);
          ("p95_latency_us", `Float stats.p95_us);
          ("p99_latency_us", `Float stats.p99_us);
        ]) results)));
    ] in
    let filename = Filename.concat output_dir
      (Printf.sprintf "ann_%s_%d_%dd_%s.json"
        (metric_to_string metric) n dim ts) in
    output_json ~filename json)

let () =
  let n = get_int_arg "n" default_n in
  let dim = get_int_arg "dim" default_dim in
  let k = get_int_arg "k" default_k in
  let n_queries = get_int_arg "queries" default_n_queries in
  let seed = get_int_arg "seed" default_seed in
  let output_dir = get_string_arg "output" "bench_results" in
  ensure_output_dir output_dir;

  let metrics = [Gvecdb.Cosine; Gvecdb.Euclidean] in
  List.iter (fun metric ->
    run_single_config ~n ~dim ~metric ~k ~n_queries ~seed ~output_dir
  ) metrics;

  Printf.printf "\nDone.\n%!"
