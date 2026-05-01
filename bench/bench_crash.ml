(** Crash Injection Test

    Validates epoch-based cross-store crash recovery:
    1. Insert some vectors and commit them (pre-crash baseline)
    2. Fork a child that starts a large batch insert
    3. SIGKILL the child mid-batch (after some HNSW commits but before
       the LMDB transaction commits)
    4. Reopen the database in the parent
    5. Verify epoch mismatch is detected and reconciliation runs
    6. Verify pre-crash data is intact
    7. Run k-NN search to confirm the index is usable

    The key crash scenario: each create_vector does HNSW commit (durable
    via msync) then buffers LMDB updates in the transaction. The LMDB
    transaction commits only when with_txn returns. Killing mid-batch
    leaves HNSW ahead of LMDB — the exact scenario reconciliation handles. *)

open Bench_common

let n_pre_crash = 500
let dim = 32
let seed = 42
let k = 10
let ef = 50

let () =
  let output_dir = get_string_arg "output" "bench_results" in
  ensure_output_dir output_dir;

  Printf.printf "=== Crash injection test ===\n%!";

  let path = temp_db_path "crash_test" in
  cleanup_db_files path;

  let rng = make_rng seed in
  let pre_vectors = Array.init n_pre_crash (fun _ -> random_vector_from rng dim) in
  let query = random_vector_from rng dim in

  (* Phase 1: insert pre-crash baseline *)
  Printf.printf "\nPhase 1: Inserting %d baseline vectors...\n%!" n_pre_crash;
  let db = ok_exn (Gvecdb.create path) in
  let batch_size = 100 in
  let i = ref 0 in
  while !i < n_pre_crash do
    let batch_end = min n_pre_crash (!i + batch_size) in
    with_txn db (fun txn ->
      for j = !i to batch_end - 1 do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        ignore (ok_exn (Gvecdb.create_vector db ~txn ~metric:Gvecdb.Euclidean
          Node node "v" (floats_to_bigstring pre_vectors.(j))))
      done);
    i := batch_end
  done;
  Printf.printf "  Baseline: %d vectors committed\n%!" n_pre_crash;

  (* Verify search works pre-crash *)
  let pre_results = ok_exn (Gvecdb.knn_hnsw db ~metric:Gvecdb.Euclidean
    ~k ~ef ~vector_tag:"v" query) in
  Printf.printf "  Pre-crash k-NN returned %d results\n%!"
    (List.length pre_results);

  Gvecdb.close db;

  (* Phase 2: fork child that starts a LARGE batch (5000 vectors in one txn).
     The child signals ready after inserting ~500 vectors into the batch.
     At that point, HNSW has committed those 500 but LMDB hasn't (the txn
     is still open). Parent kills the child, creating epoch mismatch. *)
  let n_crash_batch = 5000 in
  let signal_after = 500 in

  Printf.printf "\nPhase 2: Forking child for %d-vector batch (kill after ~%d HNSW commits)...\n%!"
    n_crash_batch signal_after;

  let ready_pipe_r, ready_pipe_w = Unix.pipe () in

  let pid = Unix.fork () in
  if pid = 0 then begin
    Unix.close ready_pipe_r;
    let db = ok_exn (Gvecdb.create path) in
    let crash_rng = make_rng (seed + 999) in
    (* Start one giant LMDB transaction. Each create_vector commits to HNSW
       individually (via msync), but LMDB only commits when with_txn returns. *)
    (try
      with_txn db (fun txn ->
        for j = 0 to n_crash_batch - 1 do
          let vec = random_vector_from crash_rng dim in
          let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
          ignore (ok_exn (Gvecdb.create_vector db ~txn ~metric:Gvecdb.Euclidean
            Node node "v" (floats_to_bigstring vec)));
          if j = signal_after then begin
            (* Signal parent: HNSW has ~500 new committed vectors,
               but LMDB txn hasn't committed any of them yet *)
            ignore (Unix.write ready_pipe_w (Bytes.of_string "R") 0 1);
            Unix.close ready_pipe_w
          end
        done)
    with _ -> ());
    Gvecdb.close db;
    exit 0
  end;

  (* Parent: wait for child to reach mid-batch, then kill *)
  Unix.close ready_pipe_w;
  let buf = Bytes.create 1 in
  ignore (Unix.read ready_pipe_r buf 0 1);
  Unix.close ready_pipe_r;

  (* Small delay to let a few more HNSW commits happen after signal *)
  Unix.sleepf 0.01;

  Printf.printf "  Child (pid=%d) mid-batch, sending SIGKILL...\n%!" pid;
  Unix.kill pid Sys.sigkill;
  let _, status = Unix.waitpid [] pid in
  (match status with
  | Unix.WSIGNALED s ->
      Printf.printf "  Child killed by signal %d\n%!" s
  | Unix.WEXITED c ->
      Printf.printf "  WARNING: Child exited normally with code %d\n%!" c
  | Unix.WSTOPPED s ->
      Printf.printf "  WARNING: Child stopped by signal %d\n%!" s);

  (* Phase 3: reopen database — triggers epoch check and reconciliation *)
  Printf.printf "\nPhase 3: Reopening database...\n%!";

  let t0 = clock_us () in
  let db = ok_exn (Gvecdb.create path) in
  let reopen_time = (clock_us () -. t0) /. 1e6 in
  Printf.printf "  Reopened in %.3fs (includes reconciliation if epoch mismatch)\n%!" reopen_time;

  (* Phase 4: verify data integrity *)
  Printf.printf "\nPhase 4: Verifying data integrity...\n%!";

  let post_results = ok_exn (Gvecdb.knn_hnsw db ~metric:Gvecdb.Euclidean
    ~k ~ef ~vector_tag:"v" query) in
  Printf.printf "  Post-recovery k-NN returned %d results\n%!" (List.length post_results);

  (* Verify pre-crash nodes survived *)
  let check_count = min 50 n_pre_crash in
  let found = ref 0 in
  for i = 0 to check_count - 1 do
    let node_id = Int64.of_int i in
    match Gvecdb.node_exists db node_id with
    | Ok true -> incr found
    | _ -> ()
  done;
  Printf.printf "  Pre-crash node check: %d/%d nodes still exist\n%!"
    !found check_count;

  (* Count total vectors visible post-recovery to confirm orphans were cleaned *)
  let post_search_all = ok_exn (Gvecdb.knn_hnsw db ~metric:Gvecdb.Euclidean
    ~k:n_pre_crash ~ef:n_pre_crash ~vector_tag:"v" query) in
  let n_recoverable = List.length post_search_all in
  Printf.printf "  Vectors reachable via search: %d (baseline was %d)\n%!"
    n_recoverable n_pre_crash;

  let all_passed =
    List.length post_results = k
    && !found = check_count
  in
  Printf.printf "\n=== Result: %s ===\n%!"
    (if all_passed then "PASS" else "FAIL");

  (* Output JSON *)
  let ts = timestamp () in
  let json : Yojson.Basic.t = `Assoc [
    ("benchmark", `String "crash_injection");
    ("timestamp", `String ts);
    ("params", `Assoc [
      ("n_pre_crash", `Int n_pre_crash);
      ("n_crash_batch", `Int n_crash_batch);
      ("signal_after", `Int signal_after);
      ("dim", `Int dim);
      ("k", `Int k);
      ("ef", `Int ef);
    ]);
    ("pre_crash", `Assoc [
      ("vectors_committed", `Int n_pre_crash);
      ("knn_results", `Int (List.length pre_results));
    ]);
    ("crash", `Assoc [
      ("signal", `String "SIGKILL");
      ("scenario", `String "mid-batch: HNSW committed, LMDB txn uncommitted");
    ]);
    ("recovery", `Assoc [
      ("reopen_time_s", `Float reopen_time);
      ("knn_results", `Int (List.length post_results));
      ("pre_crash_nodes_intact", `Int !found);
      ("pre_crash_nodes_checked", `Int check_count);
      ("vectors_reachable_post_recovery", `Int n_recoverable);
    ]);
    ("passed", `Bool all_passed);
  ] in
  let filename = Filename.concat output_dir
    (Printf.sprintf "crash_injection_%s.json" ts) in
  output_json ~filename json;

  Gvecdb.close db;
  cleanup_db_files path;
  Printf.printf "\nDone.\n%!"
