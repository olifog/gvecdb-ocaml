(** Graph Operations Benchmark

    Measures node/edge creation throughput, adjacency query
    latency, and cascade deletion cost. *)

open Bench_common

let default_n_nodes = 10000
let default_edges_per_node = 10
let default_n_queries = 1000
let default_seed = 42

(** Benchmark node creation throughput *)
let bench_node_creation ~n =
  Printf.printf "\n--- Node creation (n=%d) ---\n%!" n;
  with_bench_db "graph_nodes" @@ fun db _path ->
  let latencies = Array.init n (fun i ->
    let (_, lat) = time_us (fun () ->
      ok_exn (Gvecdb.create_node db "person")) in
    progress ~label:"create nodes" ~i ~n;
    lat
  ) in
  let stats = compute_stats latencies in
  Printf.printf "Node creation: mean=%.1fus p95=%.1fus (%.0f ops/s)\n%!"
    stats.mean_us stats.p95_us stats.qps;
  stats

(** Benchmark edge creation throughput *)
let bench_edge_creation ~n_nodes ~n_edges ~seed =
  Printf.printf "\n--- Edge creation (nodes=%d edges=%d) ---\n%!" n_nodes n_edges;
  with_bench_db "graph_edges" @@ fun db _path ->
  let rng = make_rng seed in
  let nodes = Array.init n_nodes (fun _ ->
    ok_exn (Gvecdb.create_node db "person")) in
  let edge_types = [| "knows"; "follows"; "likes" |] in
  let latencies = Array.init n_edges (fun i ->
    let src = nodes.(Random.State.int rng n_nodes) in
    let dst = nodes.(Random.State.int rng n_nodes) in
    let etype = edge_types.(Random.State.int rng (Array.length edge_types)) in
    let (_, lat) = time_us (fun () ->
      ok_exn (Gvecdb.create_edge db etype src dst)) in
    progress ~label:"create edges" ~i ~n:n_edges;
    lat
  ) in
  let stats = compute_stats latencies in
  Printf.printf "Edge creation: mean=%.1fus p95=%.1fus (%.0f ops/s)\n%!"
    stats.mean_us stats.p95_us stats.qps;
  stats

(** Benchmark adjacency queries *)
let bench_adjacency_queries ~n_nodes ~n_edges ~n_queries ~seed =
  Printf.printf "\n--- Adjacency queries (nodes=%d edges=%d queries=%d) ---\n%!"
    n_nodes n_edges n_queries;
  with_bench_db "graph_adj" @@ fun db _path ->
  let rng = make_rng seed in
  let nodes = Array.init n_nodes (fun _ ->
    ok_exn (Gvecdb.create_node db "person")) in
  let edge_types = [| "knows"; "follows"; "likes" |] in
  for i = 0 to n_edges - 1 do
    let src = nodes.(Random.State.int rng n_nodes) in
    let dst = nodes.(Random.State.int rng n_nodes) in
    let etype = edge_types.(Random.State.int rng (Array.length edge_types)) in
    ignore (ok_exn (Gvecdb.create_edge db etype src dst));
    progress ~label:"build graph" ~i ~n:n_edges
  done;

  let query_rng = make_rng (seed + 1) in

  (* Untyped outbound *)
  let outbound_lats = Array.init n_queries (fun i ->
    let node = nodes.(Random.State.int query_rng n_nodes) in
    let (_, lat) = time_us (fun () ->
      ignore (ok_exn (Gvecdb.get_outbound_edges db node ()))) in
    progress ~label:"outbound queries" ~i ~n:n_queries;
    lat
  ) in
  let outbound_stats = compute_stats outbound_lats in
  Printf.printf "Outbound: mean=%.1fus p95=%.1fus (%.0f ops/s)\n%!"
    outbound_stats.mean_us outbound_stats.p95_us outbound_stats.qps;

  (* Untyped inbound *)
  let inbound_lats = Array.init n_queries (fun i ->
    let node = nodes.(Random.State.int query_rng n_nodes) in
    let (_, lat) = time_us (fun () ->
      ignore (ok_exn (Gvecdb.get_inbound_edges db node ()))) in
    progress ~label:"inbound queries" ~i ~n:n_queries;
    lat
  ) in
  let inbound_stats = compute_stats inbound_lats in
  Printf.printf "Inbound: mean=%.1fus p95=%.1fus (%.0f ops/s)\n%!"
    inbound_stats.mean_us inbound_stats.p95_us inbound_stats.qps;

  (* Typed outbound *)
  let typed_lats = Array.init n_queries (fun i ->
    let node = nodes.(Random.State.int query_rng n_nodes) in
    let etype = edge_types.(Random.State.int query_rng (Array.length edge_types)) in
    let (_, lat) = time_us (fun () ->
      ignore (ok_exn (Gvecdb.get_outbound_edges db node ~edge_type:etype ()))) in
    progress ~label:"typed queries" ~i ~n:n_queries;
    lat
  ) in
  let typed_stats = compute_stats typed_lats in
  Printf.printf "Typed outbound: mean=%.1fus p95=%.1fus (%.0f ops/s)\n%!"
    typed_stats.mean_us typed_stats.p95_us typed_stats.qps;

  (outbound_stats, inbound_stats, typed_stats)

(** Benchmark cascade deletion at different edge counts *)
let bench_cascade_deletion ~seed =
  Printf.printf "\n--- Cascade deletion ---\n%!";
  let configs = [| (0, 0); (5, 0); (20, 0); (5, 2); (20, 5) |] in
  let dim = 32 in
  let rng = make_rng seed in
  Array.map (fun (edges_per, vecs_per) ->
    with_bench_db "graph_cascade" @@ fun db _path ->
    let n_targets = 50 in
    (* Build target nodes with specified edge/vector counts *)
    let targets = Array.init n_targets (fun _ ->
      let node = ok_exn (Gvecdb.create_node db "target") in
      for _ = 1 to vecs_per do
        with_txn db (fun txn ->
          let vec = random_vector_from rng dim in
          ignore (ok_exn (Gvecdb.create_vector db ~txn Node node "v"
            (floats_to_bigstring vec))))
      done;
      for _ = 1 to edges_per do
        let other = ok_exn (Gvecdb.create_node db "other") in
        ignore (ok_exn (Gvecdb.create_edge db "rel" node other))
      done;
      node
    ) in
    (* Delete and measure *)
    let latencies = Array.map (fun node ->
      snd (time_us (fun () ->
        ok_exn (Gvecdb.delete_node db node)))
    ) targets in
    let stats = compute_stats latencies in
    Printf.printf "  edges=%d vecs=%d: mean=%.0fus p95=%.0fus\n%!"
      edges_per vecs_per stats.mean_us stats.p95_us;
    (edges_per, vecs_per, stats)
  ) configs

let () =
  let n_nodes = get_int_arg "nodes" default_n_nodes in
  let edges_per_node = get_int_arg "edges-per-node" default_edges_per_node in
  let n_queries = get_int_arg "queries" default_n_queries in
  let seed = get_int_arg "seed" default_seed in
  let output_dir = get_string_arg "output" "bench_results" in
  ensure_output_dir output_dir;

  Printf.printf "=== Graph operations benchmark ===\n%!";

  let node_stats = bench_node_creation ~n:n_nodes in
  let n_edges = n_nodes * edges_per_node in
  let edge_stats = bench_edge_creation ~n_nodes ~n_edges ~seed in
  let (outbound, inbound, typed) =
    bench_adjacency_queries ~n_nodes ~n_edges ~n_queries ~seed in
  let cascade = bench_cascade_deletion ~seed in

  (* Output JSON *)
  let ts = timestamp () in
  let json : Yojson.Basic.t = `Assoc [
    ("benchmark", `String "graph_operations");
    ("timestamp", `String ts);
    ("params", `Assoc [
      ("n_nodes", `Int n_nodes);
      ("edges_per_node", `Int edges_per_node);
      ("n_queries", `Int n_queries);
      ("seed", `Int seed);
    ]);
    ("node_creation", `Assoc [
      ("n", `Int n_nodes);
      ("stats", stats_to_json node_stats);
    ]);
    ("edge_creation", `Assoc [
      ("n_nodes", `Int n_nodes);
      ("n_edges", `Int n_edges);
      ("stats", stats_to_json edge_stats);
    ]);
    ("adjacency_queries", `Assoc [
      ("outbound", stats_to_json outbound);
      ("inbound", stats_to_json inbound);
      ("typed_outbound", stats_to_json typed);
    ]);
    ("cascade_deletion", `List (Array.to_list (Array.map
      (fun (edges, vecs, stats) ->
        `Assoc [
          ("edges_per_node", `Int edges);
          ("vecs_per_node", `Int vecs);
          ("stats", stats_to_json stats);
        ]) cascade)));
  ] in
  let filename = Filename.concat output_dir
    (Printf.sprintf "graph_%d_%s.json" n_nodes ts) in
  output_json ~filename json;

  Printf.printf "\nDone.\n%!"
