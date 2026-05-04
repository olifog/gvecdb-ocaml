open Bench_common

let () =
  let n = get_int_arg "n" 10000 in
  let dim = get_int_arg "dim" 128 in
  let n_queries = get_int_arg "queries" 100 in
  let seed = get_int_arg "seed" 42 in
  let passes = get_int_arg "passes" 100 in
  let ef = get_int_arg "ef" 200 in
  let k = get_int_arg "k" 10 in
  let metric = Gvecdb.Euclidean in
  let dataset_path = get_string_arg "dataset" "" in

  let vectors, actual_dim =
    if dataset_path <> "" then begin
      let base_path = Filename.concat dataset_path "base.fbin" in
      let vecs, d = load_fbin base_path in
      let actual_n = min n (Array.length vecs) in
      (Array.sub vecs 0 actual_n, d)
    end else
      (generate_dataset ~seed ~n ~dim, dim)
  in
  let actual_n = Array.length vectors in

  let queries =
    if dataset_path <> "" then begin
      let query_path = Filename.concat dataset_path "queries.fbin" in
      if Sys.file_exists query_path then begin
        let q, _ = load_fbin query_path in
        Array.sub q 0 (min n_queries (Array.length q))
      end else
        generate_dataset ~seed:12345 ~n:n_queries ~dim:actual_dim
    end else
      generate_dataset ~seed:12345 ~n:n_queries ~dim:actual_dim
  in

  Printf.printf "Building index: %d vectors, %dd...\n%!" actual_n actual_dim;
  let path = temp_db_path "search_profile" in
  cleanup_db_files path;
  let db = ok_exn (Gvecdb.create path) in

  Fun.protect ~finally:(fun () -> Gvecdb.close db; cleanup_db_files path) (fun () ->
    let batch_size = 100 in
    if actual_n > 0 then
      with_txn db (fun txn ->
          let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
          ignore (ok_exn (Gvecdb.create_vector db ~txn ~metric Node node "v"
            (floats_to_bigstring vectors.(0)))));
    let i = ref 1 in
    while !i < actual_n do
      let batch_end = min actual_n (!i + batch_size) in
      let count = batch_end - !i in
      with_txn db (fun txn ->
          let node_ids = Array.init count (fun _ ->
              ok_exn (Gvecdb.create_node db ~txn "doc")) in
          let requests = List.init count (fun idx ->
              { Gvecdb.owner_kind = Node; owner_id = node_ids.(idx);
                vector_tag = "v"; data = floats_to_bigstring vectors.(!i + idx);
                normalize = true; metric }) in
          ignore (ok_exn (Gvecdb.create_vectors_batch db ~txn requests)));
      progress ~label:"build" ~i:(!i) ~n:actual_n;
      i := batch_end
    done;
    Printf.printf "\nIndex built. Starting search profiling...\n%!";

    Gc.compact ();
    let n_q = Array.length queries in
    let total = passes * n_q in
    let t0 = clock_us () in
    for pass = 0 to passes - 1 do
      for qi = 0 to n_q - 1 do
        ignore (ok_exn (Gvecdb.knn_hnsw db ~metric ~k ~ef ~vector_tag:"v"
          queries.(qi)));
        let done_count = pass * n_q + qi + 1 in
        if done_count mod 1000 = 0 then
          Printf.printf "\rsearches: %d/%d%!" done_count total
      done
    done;
    let elapsed = (clock_us () -. t0) /. 1e6 in
    Printf.printf "\n%d searches in %.2fs (%.0f QPS, ef=%d, k=%d)\n%!"
      total elapsed (float total /. elapsed) ef k)
