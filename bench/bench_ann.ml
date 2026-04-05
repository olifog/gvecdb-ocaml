open Bench_common

let default_n = 10000
let default_dim = 128
let default_k = 10
let default_n_queries = 100
let default_seed = 42
let query_seed = 12345
let default_ef_values = [| 10; 20; 50; 100; 200; 400 |]

let build_index ~vectors ~dim ~metric ~hnsw_params ~prefix =
  let n = Array.length vectors in
  let path = temp_db_path prefix in
  cleanup_db_files path;
  let db = ok_exn (Gvecdb.create path) in
  let peak_before = get_peak_rss_kb () in
  let t0 = clock_us () in
  let batch_size = 100 in
  let i = ref 0 in
  while !i < n do
    let batch_end = min n (!i + batch_size) in
    with_txn db (fun txn ->
      for j = !i to batch_end - 1 do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        ignore (ok_exn (Gvecdb.create_vector db ~txn ~metric ~hnsw_params
          Node node "v" (floats_to_bigstring vectors.(j))));
        progress ~label:(Printf.sprintf "insert %s %dd" (metric_to_string metric) dim)
          ~i:j ~n
      done);
    i := batch_end
  done;
  let build_time = (clock_us () -. t0) /. 1e6 in
  let peak_after = get_peak_rss_kb () in
  (db, path, build_time, peak_before, peak_after)

let compute_ground_truth db queries k metric =
  let n = Array.length queries in
  Array.init n (fun i ->
    progress ~label:"ground truth" ~i ~n;
    let results = ok_exn (Gvecdb.knn_brute_force db ~metric ~k queries.(i)) in
    List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) results)

let run_ef_sweep db queries ground_truth k metric ef_values =
  Array.map (fun ef ->
    Printf.printf "  ef=%d: %!" ef;
    let n_warmup = max 5 (Array.length queries / 10) in
    for i = 0 to n_warmup - 1 do
      ignore (ok_exn (Gvecdb.knn_hnsw db ~metric ~k ~ef ~vector_tag:"v"
        queries.(i mod Array.length queries)))
    done;
    let latencies = with_suppressed_gc (fun () ->
      Array.mapi (fun i query ->
        let (results, lat_us) = time_us (fun () ->
          ok_exn (Gvecdb.knn_hnsw db ~metric ~k ~ef ~vector_tag:"v" query)) in
        let ids = List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) results in
        let recall = compute_recall ~ground_truth:ground_truth.(i) ~approximate:ids in
        (lat_us, recall)
      ) queries)
    in
    let lats = Array.map fst latencies in
    let recalls = Array.map snd latencies in
    let mean_recall = Array.fold_left ( +. ) 0.0 recalls /. float (Array.length recalls) in
    let stats = compute_stats lats in
    Printf.printf "recall=%.3f qps=%.0f p50=%.0fus p99=%.0fus\n%!"
      mean_recall stats.qps stats.p50_us stats.p99_us;
    (ef, mean_recall, stats)
  ) ef_values

let run_single_config ~n ~dim ~metric ~k ~n_queries ~seed ~hnsw_params
    ~ef_values ~vectors ~queries ~output_dir =
  Printf.printf "\n=== ANN benchmark: n=%d dim=%d metric=%s k=%d m=%d ef_c=%d ===\n%!"
    n dim (metric_to_string metric) k hnsw_params.Gvecdb.Hnsw.m
    hnsw_params.Gvecdb.Hnsw.ef_construction;

  let (db, path, build_time, peak_before, peak_after) =
    build_index ~vectors ~dim ~metric ~hnsw_params
      ~prefix:(Printf.sprintf "ann_%d_%d" n dim) in

  Fun.protect ~finally:(fun () -> Gvecdb.close db; cleanup_db_files path) (fun () ->
    let index_size = get_db_size_bytes path in
    let peak_delta_kb = peak_after - peak_before in
    let bytes_per_vec = if n > 0 then index_size / n else 0 in
    Printf.printf "Build: %.2fs (%.0f vec/s), size: %d bytes (%d B/vec), peak RSS delta: %d KB\n%!"
      build_time (float n /. build_time) index_size bytes_per_vec peak_delta_kb;

    Gc.compact ();
    let ground_truth = compute_ground_truth db queries k metric in

    Gc.compact ();
    let results = run_ef_sweep db queries ground_truth k metric ef_values in

    let ts = timestamp () in
    let json : Yojson.Basic.t = `Assoc [
      ("benchmark", `String "ann_recall_vs_qps");
      ("timestamp", `String ts);
      ("system", system_metadata ());
      ("params", `Assoc [
        ("n", `Int n);
        ("dim", `Int dim);
        ("metric", `String (metric_to_string metric));
        ("k", `Int k);
        ("n_queries", `Int n_queries);
        ("seed", `Int seed);
        ("hnsw_params", hnsw_params_to_json hnsw_params);
      ]);
      ("build", `Assoc [
        ("time_s", `Float build_time);
        ("vectors_per_second", `Float (float n /. build_time));
        ("index_size_bytes", `Int index_size);
        ("bytes_per_vector_disk", `Int bytes_per_vec);
        ("peak_rss_before_kb", `Int peak_before);
        ("peak_rss_after_kb", `Int peak_after);
        ("peak_rss_delta_kb", `Int peak_delta_kb);
      ]);
      ("results", `List (Array.to_list (Array.map (fun (ef, recall, stats) ->
        `Assoc [
          ("ef", `Int ef);
          ("mean_recall", `Float recall);
          ("qps", `Float stats.qps);
          ("mean_latency_us", `Float stats.mean_us);
          ("stddev_us", `Float stats.stddev_us);
          ("p50_latency_us", `Float stats.p50_us);
          ("p95_latency_us", `Float stats.p95_us);
          ("p99_latency_us", `Float stats.p99_us);
          ("min_latency_us", `Float stats.min_us);
          ("max_latency_us", `Float stats.max_us);
        ]) results)));
    ] in
    let filename = Filename.concat output_dir
      (Printf.sprintf "ann_%s_%d_%dd_m%d_%s.json"
        (metric_to_string metric) n dim hnsw_params.m ts) in
    output_json ~filename json)

let () =
  let n = get_int_arg "n" default_n in
  let dim = get_int_arg "dim" default_dim in
  let k = get_int_arg "k" default_k in
  let n_queries = get_int_arg "queries" default_n_queries in
  let seed = get_int_arg "seed" default_seed in
  let output_dir = get_string_arg "output" "bench_results" in
  let dataset_path = get_string_arg "dataset" "" in
  let m = get_int_arg "m" 16 in
  let ef_construction = get_int_arg "ef-construction" 200 in
  ensure_output_dir output_dir;

  let hnsw_params : Gvecdb.Hnsw.params = {
    m;
    m_max = m;
    ef_construction;
    max_layers = 7;
    ml = 1.0 /. log (float_of_int m);
  } in

  let vectors, actual_dim =
    if dataset_path <> "" then begin
      Printf.printf "Loading dataset from %s...\n%!" dataset_path;
      let (vecs, d) = load_fbin dataset_path in
      let actual_n = min n (Array.length vecs) in
      (Array.sub vecs 0 actual_n, d)
    end else
      (generate_dataset ~seed ~n ~dim, dim)
  in
  let actual_n = Array.length vectors in

  let queries =
    if dataset_path <> "" then begin
      let query_path = Filename.concat (Filename.dirname dataset_path)
        "queries.fbin" in
      if Sys.file_exists query_path then begin
        Printf.printf "Loading queries from %s...\n%!" query_path;
        let (q, _) = load_fbin query_path in
        let actual_nq = min n_queries (Array.length q) in
        Array.sub q 0 actual_nq
      end else
        generate_dataset ~seed:query_seed ~n:n_queries ~dim:actual_dim
    end else
      generate_dataset ~seed:query_seed ~n:n_queries ~dim:actual_dim
  in

  let ef_str = get_string_arg "ef-values" "" in
  let ef_values =
    if ef_str <> "" then
      Array.of_list (List.map int_of_string (String.split_on_char ',' ef_str))
    else
      default_ef_values
  in

  let metrics = [Gvecdb.Cosine; Gvecdb.Euclidean] in
  List.iter (fun metric ->
    run_single_config ~n:actual_n ~dim:actual_dim ~metric ~k ~n_queries
      ~seed ~hnsw_params ~ef_values ~vectors ~queries ~output_dir
  ) metrics;

  Printf.printf "\nDone.\n%!"
