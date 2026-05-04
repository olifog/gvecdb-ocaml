open Bench_common

let default_n = 10000
let default_dim = 128
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
  (* First vector initialises the HNSW file with the desired params *)
  if n > 0 then
    with_txn db (fun txn ->
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        ignore
          (ok_exn
             (Gvecdb.create_vector db ~txn ~metric ~hnsw_params Node node "v"
                (floats_to_bigstring vectors.(0)))));
  let batch_size = 100 in
  let i = ref 1 in
  while !i < n do
    let batch_end = min n (!i + batch_size) in
    let count = batch_end - !i in
    with_txn db (fun txn ->
        let node_ids =
          Array.init count (fun _ ->
              ok_exn (Gvecdb.create_node db ~txn "doc"))
        in
        let requests =
          List.init count (fun idx ->
              {
                Gvecdb.owner_kind = Node;
                owner_id = node_ids.(idx);
                vector_tag = "v";
                data = floats_to_bigstring vectors.(!i + idx);
                normalize = true;
                metric;
              })
        in
        ignore (ok_exn (Gvecdb.create_vectors_batch db ~txn requests));
        progress
          ~label:(Printf.sprintf "insert %s %dd" (metric_to_string metric) dim)
          ~i:(batch_end - 1) ~n);
    i := batch_end
  done;
  let build_time = (clock_us () -. t0) /. 1e6 in
  let peak_after = get_peak_rss_kb () in
  (db, path, build_time, peak_before, peak_after)

let compute_ground_truth db queries k metric =
  let n = Array.length queries in
  Array.init n (fun i ->
      progress ~label:(Printf.sprintf "ground truth k=%d" k) ~i ~n;
      let results = ok_exn (Gvecdb.knn_brute_force db ~metric ~k queries.(i)) in
      List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) results)

let external_gt_to_ids (gt : int array array) ~k =
  Array.map
    (fun row ->
      let actual_k = min k (Array.length row) in
      List.init actual_k (fun j -> Int64.of_int row.(j)))
    gt

let run_ef_sweep db queries ground_truth k metric ef_values =
  Array.map
    (fun ef ->
      Printf.printf "  ef=%d: %!" ef;
      let n_warmup = max 5 (Array.length queries / 10) in
      for i = 0 to n_warmup - 1 do
        ignore
          (ok_exn
             (Gvecdb.knn_hnsw db ~metric ~k ~ef ~vector_tag:"v"
                queries.(i mod Array.length queries)))
      done;
      let latencies =
        with_suppressed_gc (fun () ->
            Array.mapi
              (fun i query ->
                let results, lat_us =
                  time_us (fun () ->
                      ok_exn
                        (Gvecdb.knn_hnsw db ~metric ~k ~ef ~vector_tag:"v" query))
                in
                let ids =
                  List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) results
                in
                let recall =
                  compute_recall ~ground_truth:ground_truth.(i) ~approximate:ids
                in
                (lat_us, recall))
              queries)
      in
      let lats = Array.map fst latencies in
      let recalls = Array.map snd latencies in
      let mean_recall =
        Array.fold_left ( +. ) 0.0 recalls /. float (Array.length recalls)
      in
      let stats = compute_stats lats in
      Printf.printf "recall=%.3f qps=%.0f p50=%.0fus p99=%.0fus\n%!" mean_recall
        stats.qps stats.p50_us stats.p99_us;
      (ef, mean_recall, stats))
    ef_values

let output_results ~n ~dim ~metric ~k ~n_queries ~seed ~hnsw_params ~build_time
    ~peak_before ~peak_after ~index_size ~results ~output_dir ?label () =
  let peak_delta_kb = peak_after - peak_before in
  let bytes_per_vec = if n > 0 then index_size / n else 0 in
  let ts = timestamp () in
  let json : Yojson.Basic.t =
    `Assoc
      [
        ("benchmark", `String "ann_recall_vs_qps");
        ("timestamp", `String ts);
        ("system", system_metadata ());
        ( "params",
          `Assoc
            [
              ("n", `Int n);
              ("dim", `Int dim);
              ("metric", `String (metric_to_string metric));
              ("k", `Int k);
              ("n_queries", `Int n_queries);
              ("seed", `Int seed);
              ("hnsw_params", hnsw_params_to_json hnsw_params);
            ] );
        ( "build",
          `Assoc
            [
              ("time_s", `Float build_time);
              ("vectors_per_second", `Float (float n /. build_time));
              ("index_size_bytes", `Int index_size);
              ("bytes_per_vector_disk", `Int bytes_per_vec);
              ("peak_rss_before_kb", `Int peak_before);
              ("peak_rss_after_kb", `Int peak_after);
              ("peak_rss_delta_kb", `Int peak_delta_kb);
            ] );
        ( "results",
          `List
            (Array.to_list
               (Array.map
                  (fun (ef, recall, stats) ->
                    `Assoc
                      [
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
                      ])
                  results)) );
      ]
  in
  let filename =
    Filename.concat output_dir
      (Printf.sprintf "ann_%s_%d_%dd_k%d_m%d_%s%s.json" (metric_to_string metric)
         n dim k hnsw_params.Gvecdb.Hnsw.m
         (match label with Some l -> "_" ^ l | None -> "")
         ts)
  in
  output_json ~filename json

let () =
  let n = get_int_arg "n" default_n in
  let dim = get_int_arg "dim" default_dim in
  let n_queries = get_int_arg "queries" default_n_queries in
  let seed = get_int_arg "seed" default_seed in
  let output_dir = get_string_arg "output" "bench_results" in
  let dataset_path = get_string_arg "dataset" "" in
  let m = get_int_arg "m" 16 in
  let ef_construction = get_int_arg "ef-construction" 200 in
  ensure_output_dir output_dir;

  let hnsw_params : Gvecdb.Hnsw.params =
    {
      m;
      m_max = m;
      ef_construction;
      max_layers = 7;
      ml = 1.0 /. log (float_of_int m);
    }
  in

  let vectors, actual_dim, dataset_metric =
    if dataset_path <> "" then (
      let base_path = Filename.concat dataset_path "base.fbin" in
      Printf.printf "Loading dataset from %s...\n%!" dataset_path;
      let vecs, d = load_fbin base_path in
      let actual_n = min n (Array.length vecs) in
      let meta =
        load_dataset_metadata (Filename.concat dataset_path "metadata.txt")
      in
      let metric =
        match Hashtbl.find_opt meta "metric" with
        | Some "euclidean" -> Some "euclidean"
        | Some "angular" -> Some "cosine"
        | _ -> None
      in
      (Array.sub vecs 0 actual_n, d, metric))
    else (generate_dataset ~seed ~n ~dim, dim, None)
  in
  let actual_n = Array.length vectors in

  let queries =
    if dataset_path <> "" then
      let query_path = Filename.concat dataset_path "queries.fbin" in
      if Sys.file_exists query_path then (
        Printf.printf "Loading queries from %s...\n%!" query_path;
        let q, _ = load_fbin query_path in
        let actual_nq = min n_queries (Array.length q) in
        Array.sub q 0 actual_nq)
      else generate_dataset ~seed:query_seed ~n:n_queries ~dim:actual_dim
    else generate_dataset ~seed:query_seed ~n:n_queries ~dim:actual_dim
  in

  let dataset_full_size =
    if dataset_path <> "" then (
      let base_path = Filename.concat dataset_path "base.fbin" in
      let ic = open_in_bin base_path in
      let buf = Bytes.create 4 in
      really_input ic buf 0 4;
      close_in ic;
      Int32.to_int (Bytes.get_int32_le buf 0))
    else n
  in

  let ground_truth_ext =
    if dataset_path <> "" && actual_n = dataset_full_size then
      let gt_path = Filename.concat dataset_path "groundtruth.ibin" in
      if Sys.file_exists gt_path then (
        Printf.printf "Loading ground truth from %s...\n%!" gt_path;
        let gt, gt_k = load_ibin gt_path in
        let actual_nq = min n_queries (Array.length gt) in
        Printf.printf "  Ground truth: %d queries, k=%d\n%!" (Array.length gt)
          gt_k;
        Some (Array.sub gt 0 actual_nq))
      else None
    else (
      if dataset_path <> "" && actual_n < dataset_full_size then
        Printf.printf
          "  Using subset (%d/%d), computing ground truth via brute force\n%!"
          actual_n dataset_full_size;
      None)
  in

  let ef_str = get_string_arg "ef-values" "" in
  let ef_values =
    if ef_str <> "" then
      Array.of_list (List.map int_of_string (String.split_on_char ',' ef_str))
    else default_ef_values
  in

  let k_values =
    let k_str = get_string_arg "k-values" "" in
    if k_str <> "" then List.map int_of_string (String.split_on_char ',' k_str)
    else [ get_int_arg "k" 10 ]
  in

  let n_runs = get_int_arg "runs" 1 in
  let do_rebuild = get_bool_arg "rebuild" in

  let metrics =
    match dataset_metric with
    | Some "euclidean" -> [ Gvecdb.Euclidean ]
    | Some "cosine" -> [ Gvecdb.Cosine ]
    | _ -> [ Gvecdb.Euclidean; Gvecdb.Cosine ]
  in

  List.iter
    (fun metric ->
      Printf.printf
        "\n=== Building index: n=%d dim=%d metric=%s m=%d ef_c=%d ===\n%!"
        actual_n actual_dim (metric_to_string metric) hnsw_params.m
        hnsw_params.ef_construction;

      let db, path, build_time, peak_before, peak_after =
        build_index ~vectors ~dim:actual_dim ~metric ~hnsw_params
          ~prefix:(Printf.sprintf "ann_%d_%d" actual_n actual_dim)
      in

      Fun.protect
        ~finally:(fun () ->
          Gvecdb.close db;
          cleanup_db_files path)
        (fun () ->
          let index_size = get_db_size_bytes path in
          let peak_delta_kb = peak_after - peak_before in
          let bytes_per_vec =
            if actual_n > 0 then index_size / actual_n else 0
          in
          Printf.printf
            "Build: %.2fs (%.0f vec/s), size: %d bytes (%d B/vec), peak RSS \
             delta: %d KB\n\
             %!"
            build_time
            (float actual_n /. build_time)
            index_size bytes_per_vec peak_delta_kb;

          Gc.compact ();

          let run_sweeps ?label () =
            List.iter
              (fun k ->
                Printf.printf "\n--- Sweep k=%d metric=%s%s ---\n%!" k
                  (metric_to_string metric)
                  (match label with Some l -> " (" ^ l ^ ")" | None -> "");
                let ground_truth =
                  match ground_truth_ext with
                  | Some gt -> external_gt_to_ids gt ~k
                  | None -> compute_ground_truth db queries k metric
                in
                Gc.compact ();
                let results =
                  run_ef_sweep db queries ground_truth k metric ef_values
                in
                output_results ~n:actual_n ~dim:actual_dim ~metric ~k
                  ~n_queries:(Array.length queries) ~seed ~hnsw_params
                  ~build_time ~peak_before ~peak_after ~index_size ~results
                  ~output_dir ?label ())
              k_values
          in

          for run = 1 to n_runs do
            let label =
              if n_runs > 1 then Some (Printf.sprintf "run%d" run)
              else None
            in
            run_sweeps ?label ()
          done;

          if do_rebuild then begin
            Printf.printf "\n=== Rebuilding HNSW index ===\n%!";
            let pre_size = get_db_size_bytes path in
            let t0 = clock_us () in
            ok_exn (Gvecdb.rebuild_hnsw_index db ~vector_tag:"v" ());
            let rebuild_time = (clock_us () -. t0) /. 1e6 in
            let post_size = get_db_size_bytes path in
            Printf.printf
              "Rebuild: %.2fs (%.0f vec/s), size: %d -> %d bytes (%.1fx compaction)\n%!"
              rebuild_time (float actual_n /. rebuild_time)
              pre_size post_size
              (float pre_size /. float post_size);

            for run = 1 to n_runs do
              let label =
                if n_runs > 1 then
                  Some (Printf.sprintf "rebuilt_run%d" run)
                else Some "rebuilt"
              in
              run_sweeps ?label ()
            done
          end))
    metrics;

  Printf.printf "\nDone.\n%!"
