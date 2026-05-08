open Bench_common

let default_n = 10000
let default_dim = 128
let default_k = 10
let default_ef = 50
let default_seed = 42
let query_seed = 12345
let default_n_queries = 200
let deletion_ratios = [| 0.0; 0.10; 0.25; 0.50; 0.75 |]

let () =
  let n = get_int_arg "n" default_n in
  let dim = get_int_arg "dim" default_dim in
  let k = get_int_arg "k" default_k in
  let ef = get_int_arg "ef" default_ef in
  let seed = get_int_arg "seed" default_seed in
  let n_queries = get_int_arg "queries" default_n_queries in
  let output_dir = get_string_arg "output" "bench_results" in
  ensure_output_dir output_dir;

  let metric = Gvecdb.Euclidean in
  let hnsw_params : Gvecdb.Hnsw.params =
    { m = 16; m_max = 16; ef_construction = 200; max_layers = 7;
      ml = 1.0 /. log (float_of_int 16) }
  in

  Printf.printf "=== Deletion Recall Benchmark ===\n%!";
  Printf.printf "n=%d dim=%d k=%d ef=%d\n%!" n dim k ef;

  let rng = make_rng seed in
  let vectors = Array.init n (fun _ -> random_vector_from rng dim) in
  let queries = generate_dataset ~seed:query_seed ~n:n_queries ~dim in

  let path = temp_db_path "deletion" in
  cleanup_db_files path;
  let db = ok_exn (Gvecdb.create path) in

  Fun.protect
    ~finally:(fun () -> Gvecdb.close db; cleanup_db_files path)
    (fun () ->
      Printf.printf "Building index with %d vectors...\n%!" n;
      let node_ids = Array.make n 0L in
      let vector_ids = Array.make n 0L in

      let batch_size = 100 in
      let i = ref 0 in
      while !i < n do
        let batch_end = min n (!i + batch_size) in
        let count = batch_end - !i in
        with_txn db (fun txn ->
            for idx = 0 to count - 1 do
              let pos = !i + idx in
              let nid = ok_exn (Gvecdb.create_node db ~txn "doc") in
              node_ids.(pos) <- nid;
              if pos = 0 then
                vector_ids.(pos) <-
                  ok_exn (Gvecdb.create_vector db ~txn ~metric ~hnsw_params
                            Node nid "v" (floats_to_bigstring vectors.(pos)))
              else begin
                let requests = [{
                  Gvecdb.owner_kind = Node; owner_id = nid;
                  vector_tag = "v"; data = floats_to_bigstring vectors.(pos);
                  normalize = true; metric;
                }] in
                match ok_exn (Gvecdb.create_vectors_batch db ~txn requests) with
                | [vid] -> vector_ids.(pos) <- vid
                | _ -> failwith "batch returned wrong count"
              end
            done);
        progress ~label:"insert" ~i:(batch_end - 1) ~n;
        i := batch_end
      done;

      Printf.printf "\nComputing ground truth (brute force, all vectors)...\n%!";
      let full_gt =
        Array.init n_queries (fun i ->
            progress ~label:"ground truth" ~i ~n:n_queries;
            let results = ok_exn (Gvecdb.knn_brute_force db ~metric ~k queries.(i)) in
            List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) results)
      in

      Printf.printf "\nBaseline recall (0%% deleted)...\n%!";
      let baseline_recall =
        with_suppressed_gc (fun () ->
            let total = ref 0.0 in
            for i = 0 to n_queries - 1 do
              let results =
                ok_exn (Gvecdb.knn_hnsw db ~metric ~k ~ef ~vector_tag:"v" queries.(i))
              in
              let ids = List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) results in
              total := !total +. compute_recall ~ground_truth:full_gt.(i) ~approximate:ids
            done;
            !total /. float n_queries)
      in
      Printf.printf "  Baseline recall@%d: %.4f\n%!" k baseline_recall;

      let delete_rng = make_rng 99 in
      let shuffled = Array.init n (fun i -> i) in
      for i = n - 1 downto 1 do
        let j = Random.State.int delete_rng (i + 1) in
        let tmp = shuffled.(i) in
        shuffled.(i) <- shuffled.(j);
        shuffled.(j) <- tmp
      done;

      let results = Array.make (Array.length deletion_ratios) (0.0, 0.0, 0.0) in
      results.(0) <- (0.0, baseline_recall, baseline_recall);

      let deleted_so_far = ref 0 in

      for ri = 1 to Array.length deletion_ratios - 1 do
        let target_deleted = int_of_float (deletion_ratios.(ri) *. float n) in
        let to_delete = target_deleted - !deleted_so_far in
        Printf.printf "\nDeleting %d vectors (total %.0f%%)...\n%!" to_delete
          (deletion_ratios.(ri) *. 100.0);

        let batch = 50 in
        let d = ref 0 in
        while !d < to_delete do
          let batch_end = min to_delete (!d + batch) in
          with_txn db (fun txn ->
              for di = !d to batch_end - 1 do
                let idx = shuffled.(!deleted_so_far + di) in
                ignore (ok_exn (Gvecdb.delete_vector db ~txn vector_ids.(idx)))
              done);
          d := batch_end
        done;
        deleted_so_far := target_deleted;

        let surviving_set =
          let s = Hashtbl.create (n - target_deleted) in
          for i = 0 to n - 1 do
            let is_deleted = ref false in
            for d = 0 to target_deleted - 1 do
              if shuffled.(d) = i then is_deleted := true
            done;
            if not !is_deleted then Hashtbl.replace s vector_ids.(i) true
          done;
          s
        in

        let gt_after_deletion =
          Array.init n_queries (fun qi ->
              let results =
                ok_exn (Gvecdb.knn_brute_force db ~metric ~k queries.(qi))
              in
              List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) results)
        in

        let recall_vs_surviving =
          with_suppressed_gc (fun () ->
              let total = ref 0.0 in
              for i = 0 to n_queries - 1 do
                let results =
                  ok_exn (Gvecdb.knn_hnsw db ~metric ~k ~ef ~vector_tag:"v" queries.(i))
                in
                let ids = List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) results in
                let valid_ids = List.filter (fun id -> Hashtbl.mem surviving_set id) ids in
                total := !total +.
                  compute_recall ~ground_truth:gt_after_deletion.(i) ~approximate:valid_ids
              done;
              !total /. float n_queries)
        in

        let recall_vs_original =
          with_suppressed_gc (fun () ->
              let total = ref 0.0 in
              for i = 0 to n_queries - 1 do
                let results =
                  ok_exn (Gvecdb.knn_hnsw db ~metric ~k ~ef ~vector_tag:"v" queries.(i))
                in
                let ids = List.map (fun (r : Gvecdb.knn_result) -> r.vector_id) results in
                total := !total +.
                  compute_recall ~ground_truth:gt_after_deletion.(i) ~approximate:ids
              done;
              !total /. float n_queries)
        in

        Printf.printf "  recall@%d vs surviving (brute force): %.4f\n%!" k recall_vs_surviving;
        Printf.printf "  recall@%d vs surviving (hnsw): %.4f\n%!" k recall_vs_original;
        results.(ri) <- (deletion_ratios.(ri), recall_vs_surviving, recall_vs_original)
      done;

      Printf.printf "\n=== Summary ===\n%!";
      Printf.printf "| Deletion %% | Recall (HNSW vs BF on survivors) | Recall (raw HNSW) |\n%!";
      Printf.printf "|------------|----------------------------------|-------------------|\n%!";
      Array.iter (fun (ratio, recall_vs_bf, recall_raw) ->
          Printf.printf "| %10.0f%% | %32.4f | %17.4f |\n%!"
            (ratio *. 100.0) recall_vs_bf recall_raw)
        results;

      let ts = timestamp () in
      let json : Yojson.Basic.t =
        `Assoc [
          ("benchmark", `String "deletion_recall");
          ("timestamp", `String ts);
          ("system", system_metadata ());
          ("params", `Assoc [
              ("n", `Int n); ("dim", `Int dim); ("k", `Int k);
              ("ef", `Int ef); ("n_queries", `Int n_queries);
              ("metric", `String (metric_to_string metric));
              ("hnsw_params", hnsw_params_to_json hnsw_params);
            ]);
          ("results", `List (Array.to_list (Array.map (fun (ratio, recall_bf, recall_raw) ->
               `Assoc [
                 ("deletion_ratio", `Float ratio);
                 ("recall_vs_brute_force", `Float recall_bf);
                 ("recall_hnsw_raw", `Float recall_raw);
               ]) results)));
        ]
      in
      let filename = Filename.concat output_dir
          (Printf.sprintf "deletion_recall_%d_%dd_k%d_%s.json" n dim k ts) in
      output_json ~filename json);

  Printf.printf "\nDone.\n%!"
