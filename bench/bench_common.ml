(** Shared benchmark infrastructure *)

(** {1 Timing} *)

let time_us f =
  let t0 = Unix.gettimeofday () in
  let result = f () in
  let t1 = Unix.gettimeofday () in
  (result, (t1 -. t0) *. 1_000_000.0)

let benchmark_n n f =
  Array.init n (fun _ -> snd (time_us f))

(** {1 Statistics} *)

type stats = {
  mean_us : float;
  p50_us : float;
  p95_us : float;
  p99_us : float;
  qps : float;
  count : int;
}

let compute_stats latencies =
  let n = Array.length latencies in
  if n = 0 then { mean_us = 0.0; p50_us = 0.0; p95_us = 0.0; p99_us = 0.0; qps = 0.0; count = 0 }
  else
    let sorted = Array.copy latencies in
    Array.sort Float.compare sorted;
    let total = Array.fold_left ( +. ) 0.0 sorted in
    let idx p = min (n - 1) (int_of_float (float (n - 1) *. p)) in
    {
      mean_us = total /. float n;
      p50_us = sorted.(idx 0.50);
      p95_us = sorted.(idx 0.95);
      p99_us = sorted.(idx 0.99);
      qps = float n /. (total /. 1_000_000.0);
      count = n;
    }

let stats_to_json s : Yojson.Basic.t =
  `Assoc [
    ("mean_latency_us", `Float s.mean_us);
    ("p50_latency_us", `Float s.p50_us);
    ("p95_latency_us", `Float s.p95_us);
    ("p99_latency_us", `Float s.p99_us);
    ("qps", `Float s.qps);
    ("count", `Int s.count);
  ]

(** {1 Recall} *)

module Int64Set = Set.Make (Int64)

let compute_recall ~ground_truth ~approximate =
  let gt_set = List.fold_left (fun s id -> Int64Set.add id s) Int64Set.empty ground_truth in
  let n_gt = List.length ground_truth in
  if n_gt = 0 then 1.0
  else
    let matches = List.fold_left (fun acc id ->
      if Int64Set.mem id gt_set then acc + 1 else acc
    ) 0 approximate in
    float matches /. float n_gt

(** {1 Vector generation} *)

let make_rng seed = Random.State.make [| seed |]

let random_vector_from rng dim =
  Array.init dim (fun _ -> Random.State.float rng 2.0 -. 1.0)

let generate_dataset ~seed ~n ~dim =
  let rng = make_rng seed in
  Array.init n (fun _ -> random_vector_from rng dim)

let floats_to_bigstring (arr : float array) =
  let n = Array.length arr in
  let bs = Bigstringaf.create (n * 4) in
  for i = 0 to n - 1 do
    Bigstringaf.set_int32_le bs (i * 4) (Int32.bits_of_float arr.(i))
  done;
  bs

(** {1 Database lifecycle} *)

let ok_exn = function
  | Ok x -> x
  | Error e -> failwith (Gvecdb.Error.to_string e)

let with_txn db f =
  match Gvecdb.with_transaction db f with
  | Some x -> x
  | None -> failwith "transaction aborted"

let temp_db_path prefix =
  Filename.concat (Filename.get_temp_dir_name ())
    (Printf.sprintf "bench_%s_%d.db" prefix (Unix.getpid ()))

let cleanup_db_files path =
  (try Sys.remove path with _ -> ());
  let base = Filename.remove_extension path in
  (try Sys.remove (base ^ ".vectors") with _ -> ());
  let hnsw_dir = base ^ ".hnsw" in
  (try
     if Sys.file_exists hnsw_dir && Sys.is_directory hnsw_dir then begin
       Array.iter
         (fun f -> try Sys.remove (Filename.concat hnsw_dir f) with _ -> ())
         (Sys.readdir hnsw_dir);
       Unix.rmdir hnsw_dir
     end
   with _ -> ())

let with_bench_db prefix f =
  let path = temp_db_path prefix in
  cleanup_db_files path;
  let db = ok_exn (Gvecdb.create path) in
  Fun.protect
    ~finally:(fun () -> Gvecdb.close db; cleanup_db_files path)
    (fun () -> f db path)

(** {1 Disk size measurement} *)

let get_db_size_bytes path =
  let total = ref 0 in
  let add f = try total := !total + (Unix.stat f).st_size with _ -> () in
  add path;
  add (path ^ "-lock");
  let base = Filename.remove_extension path in
  add (base ^ ".vectors");
  let hnsw_dir = base ^ ".hnsw" in
  (try
     if Sys.file_exists hnsw_dir && Sys.is_directory hnsw_dir then
       Array.iter (fun f -> add (Filename.concat hnsw_dir f)) (Sys.readdir hnsw_dir)
   with _ -> ());
  !total

(** {1 JSON output} *)

let output_json ~filename json =
  let s = Yojson.Basic.pretty_to_string json in
  let oc = open_out filename in
  Fun.protect ~finally:(fun () -> close_out oc) (fun () ->
    output_string oc s;
    output_char oc '\n');
  Printf.printf "Results written to %s\n%!" filename

let timestamp () =
  let t = Unix.localtime (Unix.gettimeofday ()) in
  Printf.sprintf "%04d%02d%02d_%02d%02d%02d"
    (t.tm_year + 1900) (t.tm_mon + 1) t.tm_mday
    t.tm_hour t.tm_min t.tm_sec

(** {1 CLI argument parsing} *)

let get_int_arg name default =
  let prefix = "--" ^ name ^ "=" in
  let plen = String.length prefix in
  try
    let arg = Array.to_list Sys.argv |> List.find (fun s ->
      String.length s > plen && String.sub s 0 plen = prefix) in
    int_of_string (String.sub arg plen (String.length arg - plen))
  with Not_found -> default

let get_string_arg name default =
  let prefix = "--" ^ name ^ "=" in
  let plen = String.length prefix in
  try
    let arg = Array.to_list Sys.argv |> List.find (fun s ->
      String.length s > plen && String.sub s 0 plen = prefix) in
    String.sub arg plen (String.length arg - plen)
  with Not_found -> default

let ensure_output_dir dir =
  (try Unix.mkdir dir 0o755 with Unix.Unix_error (Unix.EEXIST, _, _) -> ())

(** {1 Progress reporting} *)

let progress ~label ~i ~n =
  let interval = max 1 (n / 20) in
  if i mod interval = 0 || i = n - 1 then
    Printf.printf "\r[%s] %d/%d%!" label (i + 1) n;
  if i = n - 1 then print_newline ()

(** {1 Metric helpers} *)

let metric_to_string = function
  | Gvecdb.Euclidean -> "euclidean"
  | Gvecdb.Cosine -> "cosine"
  | Gvecdb.DotProduct -> "dot_product"

let hnsw_params_to_json () : Yojson.Basic.t =
  let p = Gvecdb.Hnsw.default_params in
  `Assoc [
    ("m", `Int p.m);
    ("m_max", `Int p.m_max);
    ("ef_construction", `Int p.ef_construction);
    ("max_layers", `Int p.max_layers);
  ]
