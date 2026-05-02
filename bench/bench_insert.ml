(** Insertion Throughput Benchmark

    Measures:
    1. Per-vector insert throughput (each insert commits to HNSW individually)
    2. Batch-100 transaction throughput (100 vectors per LMDB txn, each still
       individually committed to HNSW)
    3. Rebuild throughput (rebuild_hnsw_index commits every 1000 vectors to
       HNSW)

    The rebuild path measures what batch HNSW commits would achieve. *)

open Bench_common

let default_n = 10000
let default_dim = 128
let default_batch_size = 100
let default_seed = 42

type batch_result = {
  cumulative : int;
  batch_size : int;
  batch_time_s : float;
  throughput_vps : float;
}

let batch_result_to_json b : Yojson.Basic.t =
  `Assoc
    [
      ("cumulative", `Int b.cumulative);
      ("batch_size", `Int b.batch_size);
      ("batch_time_s", `Float b.batch_time_s);
      ("throughput_vps", `Float b.throughput_vps);
    ]

(** Insert N vectors using create_vectors_batch (one HNSW commit per LMDB txn batch) *)
let bench_batched ~vectors ~batch_size =
  let n = Array.length vectors in
  with_bench_db "insert_batch" @@ fun db path ->
  let results = ref [] in
  let i = ref 0 in
  let total_t0 = clock_us () in
  while !i < n do
    let batch_end = min n (!i + batch_size) in
    let count = batch_end - !i in
    let t0 = clock_us () in
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
                metric = Gvecdb.Euclidean;
              })
        in
        ignore (ok_exn (Gvecdb.create_vectors_batch db ~txn requests)));
    let t1 = clock_us () in
    let batch_time = (t1 -. t0) /. 1e6 in
    results :=
      {
        cumulative = batch_end;
        batch_size = count;
        batch_time_s = batch_time;
        throughput_vps = float count /. batch_time;
      }
      :: !results;
    progress ~label:"batch insert" ~i:!i ~n;
    i := batch_end
  done;
  let total_time = (clock_us () -. total_t0) /. 1e6 in
  let index_size = get_db_size_bytes path in
  (List.rev !results, total_time, index_size)

(** Insert N vectors one per transaction for overhead comparison *)
let bench_single_txn ~vectors ~n_sample =
  let n = min n_sample (Array.length vectors) in
  with_bench_db "insert_single" @@ fun db _path ->
  let latencies =
    Array.init n (fun i ->
        let (), lat_us =
          time_us (fun () ->
              with_txn db (fun txn ->
                  let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
                  ignore
                    (ok_exn
                       (Gvecdb.create_vector db ~txn Node node "v"
                          (floats_to_bigstring vectors.(i))))))
        in
        progress ~label:"single-txn insert" ~i ~n;
        lat_us)
  in
  compute_stats latencies

(** Insert vectors normally, then measure rebuild_hnsw_index throughput (commits
    every 1000 vectors to HNSW) separately *)
let bench_rebuild ~vectors =
  let n = Array.length vectors in
  with_bench_db "insert_rebuild" @@ fun db path ->
  let batch_size = 100 in
  let i = ref 0 in
  while !i < n do
    let batch_end = min n (!i + batch_size) in
    with_txn db (fun txn ->
        for j = !i to batch_end - 1 do
          let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
          ignore
            (ok_exn
               (Gvecdb.create_vector db ~txn Node node "v"
                  (floats_to_bigstring vectors.(j))))
        done);
    progress ~label:"insert (initial)" ~i:!i ~n;
    i := batch_end
  done;
  Printf.printf "\n  Initial build complete, now timing rebuild...\n%!";
  let rebuild_t0 = clock_us () in
  ok_exn (Gvecdb.rebuild_hnsw_index db ~vector_tag:"v" ());
  let rebuild_time = (clock_us () -. rebuild_t0) /. 1e6 in
  let index_size = get_db_size_bytes path in
  Printf.printf "  Rebuild: %.2fs (%.0f vec/s)\n%!" rebuild_time
    (float n /. rebuild_time);
  (rebuild_time, index_size)

let () =
  let n = get_int_arg "n" default_n in
  let dim = get_int_arg "dim" default_dim in
  let batch_size = get_int_arg "batch" default_batch_size in
  let seed = get_int_arg "seed" default_seed in
  let output_dir = get_string_arg "output" "bench_results" in
  ensure_output_dir output_dir;

  Printf.printf "=== Insertion benchmark: n=%d dim=%d batch=%d ===\n%!" n dim
    batch_size;

  let vectors = generate_dataset ~seed ~n ~dim in

  (* Batched insertion (per-vector HNSW commit, batched LMDB txns) *)
  Printf.printf "\n--- Batched insertion (batch_size=%d) ---\n%!" batch_size;
  let batches, total_time, index_size = bench_batched ~vectors ~batch_size in
  Printf.printf "Total: %.2fs (%.0f vec/s), size: %d bytes\n%!" total_time
    (float n /. total_time)
    index_size;

  (* Single-txn insertion (sample of first 1000 to keep it reasonable) *)
  let single_n = min 1000 n in
  Printf.printf "\n--- Single-txn insertion (n=%d) ---\n%!" single_n;
  let single_stats = bench_single_txn ~vectors ~n_sample:single_n in
  Printf.printf "Single-txn: mean=%.0fus p95=%.0fus (%.0f vec/s)\n%!"
    single_stats.mean_us single_stats.p95_us single_stats.qps;

  (* Rebuild path (batch-1000 HNSW commits) *)
  Printf.printf "\n--- Rebuild path (batch-1000 HNSW commits) ---\n%!";
  let rebuild_time, rebuild_size = bench_rebuild ~vectors in

  let rebuild_vps = float n /. rebuild_time in
  let batched_vps = float n /. total_time in
  let speedup = rebuild_vps /. batched_vps in
  Printf.printf "\n--- Summary ---\n%!";
  Printf.printf "Per-vector (batch=%d txns):  %.0f vec/s\n%!" batch_size
    batched_vps;
  Printf.printf "Rebuild (batch-1000 HNSW):  %.0f vec/s (%.1fx faster)\n%!"
    rebuild_vps speedup;

  (* Output JSON *)
  let ts = timestamp () in
  let json : Yojson.Basic.t =
    `Assoc
      [
        ("benchmark", `String "insertion_throughput");
        ("timestamp", `String ts);
        ("system", system_metadata ());
        ( "params",
          `Assoc
            [
              ("n", `Int n);
              ("dim", `Int dim);
              ("batch_size", `Int batch_size);
              ("seed", `Int seed);
              ("hnsw_params", hnsw_params_to_json Gvecdb.Hnsw.default_params);
            ] );
        ( "batched",
          `Assoc
            [
              ("total_time_s", `Float total_time);
              ("total_throughput_vps", `Float batched_vps);
              ("index_size_bytes", `Int index_size);
              ("batches", `List (List.map batch_result_to_json batches));
            ] );
        ( "single_txn",
          `Assoc [ ("n", `Int single_n); ("stats", stats_to_json single_stats) ]
        );
        ( "rebuild",
          `Assoc
            [
              ("rebuild_time_s", `Float rebuild_time);
              ("rebuild_throughput_vps", `Float rebuild_vps);
              ("index_size_bytes", `Int rebuild_size);
              ("speedup_vs_batched", `Float speedup);
            ] );
      ]
  in
  let filename =
    Filename.concat output_dir (Printf.sprintf "insert_%d_%dd_%s.json" n dim ts)
  in
  output_json ~filename json;

  Printf.printf "\nDone.\n%!"
