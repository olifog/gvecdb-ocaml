(** Concurrent Reader Isolation Test

    Validates MVCC reader isolation under true parallelism using OCaml Domains
    (OS-level threads). Measures:
    1. Read-only k-NN latency distribution (baseline, single domain)
    2. Read k-NN latency distribution while a writer domain is active
    3. Compares p50/p99 to confirm reads aren't blocked by writes *)

open Bench_common

let n_base = 5000
let dim = 128
let seed = 42
let query_seed = 12345
let n_queries = 100
let k = 10
let ef = 100
let n_read_samples = 2000
let n_write_vectors = 500

let measure_read_latencies db queries ~n_samples =
  let n_q = Array.length queries in
  Array.init n_samples (fun i ->
      let q = queries.(i mod n_q) in
      let _, lat_us =
        time_us (fun () ->
            ok_exn
              (Gvecdb.knn_hnsw db ~metric:Gvecdb.Euclidean ~k ~ef
                 ~vector_tag:"v" q))
      in
      lat_us)

let () =
  let output_dir = get_string_arg "output" "bench_results" in
  ensure_output_dir output_dir;

  Printf.printf "=== Concurrent reader isolation test (Domain-parallel) ===\n%!";

  let rng = make_rng seed in
  let vectors = Array.init n_base (fun _ -> random_vector_from rng dim) in
  let queries = generate_dataset ~seed:query_seed ~n:n_queries ~dim in
  let write_vectors =
    Array.init n_write_vectors (fun _ -> random_vector_from rng dim)
  in

  Printf.printf "Building index with %d vectors...\n%!" n_base;
  let path = temp_db_path "concurrent" in
  cleanup_db_files path;
  let db = ok_exn (Gvecdb.create path) in

  Fun.protect
    ~finally:(fun () ->
      Gvecdb.close db;
      cleanup_db_files path)
    (fun () ->
      let batch_size = 100 in
      let i = ref 0 in
      while !i < n_base do
        let batch_end = min n_base (!i + batch_size) in
        with_txn db (fun txn ->
            for j = !i to batch_end - 1 do
              let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
              ignore
                (ok_exn
                   (Gvecdb.create_vector db ~txn ~metric:Gvecdb.Euclidean Node
                      node "v"
                      (floats_to_bigstring vectors.(j))))
            done);
        i := batch_end
      done;
      Gc.compact ();

      (* Warmup *)
      for qi = 0 to min 50 (n_queries - 1) do
        ignore
          (ok_exn
             (Gvecdb.knn_hnsw db ~metric:Gvecdb.Euclidean ~k ~ef ~vector_tag:"v"
                queries.(qi)))
      done;

      (* Phase 1: read-only latency (single domain, no contention) *)
      Printf.printf "Phase 1: Measuring read-only latency (%d queries)...\n%!"
        n_read_samples;
      let readonly_lats =
        measure_read_latencies db queries ~n_samples:n_read_samples
      in
      let readonly_stats = compute_stats readonly_lats in
      Printf.printf "  Read-only: p50=%.0fus p99=%.0fus QPS=%.0f\n%!"
        readonly_stats.p50_us readonly_stats.p99_us readonly_stats.qps;

      (* Phase 2: read latency with concurrent writer on a separate Domain *)
      Gc.compact ();
      Printf.printf
        "Phase 2: Measuring read latency with concurrent writer domain...\n%!";

      let writer_done = Atomic.make false in
      let writer_count = Atomic.make 0 in

      let writer_domain =
        Domain.spawn (fun () ->
            let wi = ref 0 in
            while (not (Atomic.get writer_done)) && !wi < n_write_vectors do
              with_txn db (fun txn ->
                  let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
                  ignore
                    (ok_exn
                       (Gvecdb.create_vector db ~txn ~metric:Gvecdb.Euclidean
                          Node node "v"
                          (floats_to_bigstring write_vectors.(!wi)))));
              incr wi;
              Atomic.set writer_count !wi
            done;
            Atomic.get writer_count)
      in

      let concurrent_lats =
        measure_read_latencies db queries ~n_samples:n_read_samples
      in
      Atomic.set writer_done true;
      let vectors_written = Domain.join writer_domain in
      let concurrent_stats = compute_stats concurrent_lats in

      Printf.printf
        "  Concurrent: p50=%.0fus p99=%.0fus QPS=%.0f (writer inserted %d)\n%!"
        concurrent_stats.p50_us concurrent_stats.p99_us concurrent_stats.qps
        vectors_written;

      let p50_ratio = concurrent_stats.p50_us /. readonly_stats.p50_us in
      let qps_ratio = concurrent_stats.qps /. readonly_stats.qps in
      Printf.printf "\n  p50 ratio (concurrent/readonly): %.2f\n%!" p50_ratio;
      Printf.printf "  QPS ratio: %.2f\n%!" qps_ratio;
      Printf.printf "  %s\n%!"
        (if p50_ratio < 1.5 then
           "PASS: reads not degraded (MVCC isolation confirmed)"
         else if p50_ratio < 3.0 then "MARGINAL: some degradation observed"
         else "FAIL: significant read degradation during writes");

      (* Output JSON *)
      let ts = timestamp () in
      let json : Yojson.Basic.t =
        `Assoc
          [
            ("benchmark", `String "concurrent_reader_isolation");
            ("timestamp", `String ts);
            ("system", system_metadata ());
            ( "params",
              `Assoc
                [
                  ("n_base", `Int n_base);
                  ("dim", `Int dim);
                  ("k", `Int k);
                  ("ef", `Int ef);
                  ("n_read_samples", `Int n_read_samples);
                  ("n_write_vectors", `Int n_write_vectors);
                  ("parallelism", `String "Domain (true OS-level parallelism)");
                ] );
            ("readonly", stats_to_json readonly_stats);
            ( "concurrent",
              `Assoc
                [
                  ("stats", stats_to_json concurrent_stats);
                  ("vectors_written_during", `Int vectors_written);
                ] );
            ("p50_ratio", `Float p50_ratio);
            ("qps_ratio", `Float qps_ratio);
          ]
      in
      let filename =
        Filename.concat output_dir
          (Printf.sprintf "concurrent_reader_%s.json" ts)
      in
      output_json ~filename json);

  Printf.printf "\nDone.\n%!"
