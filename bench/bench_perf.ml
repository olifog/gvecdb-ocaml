(* search-only benchmark for perf profiling *)

open Bench_common

let dim = 128
let n = 10000
let seed = 42
let query_seed = 12345
let n_queries = 100
let ef = 200

let () =
  let vectors = generate_dataset ~seed ~n ~dim in
  let queries = generate_dataset ~seed:query_seed ~n:n_queries ~dim in

  let path = temp_db_path "perf_profile" in
  cleanup_db_files path;
  let db = ok_exn (Gvecdb.create path) in
  Fun.protect ~finally:(fun () -> Gvecdb.close db; cleanup_db_files path)
    (fun () ->
      let batch_size = 100 in
      let i = ref 0 in
      while !i < n do
        let batch_end = min n (!i + batch_size) in
        with_txn db (fun txn ->
          for j = !i to batch_end - 1 do
            let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
            ignore (ok_exn (Gvecdb.create_vector db ~txn ~metric:Gvecdb.Cosine
              Node node "v" (floats_to_bigstring vectors.(j))))
          done);
        i := batch_end
      done;
      Printf.printf "Built index: %d vectors\n%!" n;
      Gc.compact ();

      let passes = 200 in
      let total = passes * n_queries in
      let t0 = clock_us () in
      for pass = 0 to passes - 1 do
        for qi = 0 to n_queries - 1 do
          ignore (ok_exn (Gvecdb.knn_hnsw db ~metric:Gvecdb.Cosine ~k:10 ~ef
            ~vector_tag:"v" queries.(qi)));
          let done_count = pass * n_queries + qi + 1 in
          if done_count mod 500 = 0 then
            Printf.printf "\rsearches: %d/%d%!" done_count total
        done
      done;
      let elapsed = (clock_us () -. t0) /. 1e6 in
      Printf.printf "\n%d searches in %.2fs (%.0f QPS, ef=%d)\n%!"
        total elapsed (float total /. elapsed) ef)
