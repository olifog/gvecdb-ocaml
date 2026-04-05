external clock_monotonic_ns : unit -> float = "bench_clock_monotonic_ns"

let clock_us () = clock_monotonic_ns () /. 1e3

let time_us f =
  let t0 = clock_us () in
  let result = f () in
  let t1 = clock_us () in
  (result, t1 -. t0)

let warmup ?(min_us = 500_000.0) ?(min_iters = 10) f =
  let t0 = clock_us () in
  let i = ref 0 in
  while !i < min_iters || clock_us () -. t0 < min_us do
    ignore (f ());
    incr i
  done;
  !i

let with_suppressed_gc f =
  Gc.compact ();
  let old = Gc.get () in
  Gc.set { old with
    minor_heap_size = 4_194_304;
    space_overhead = 1_000_000;
  };
  Fun.protect ~finally:(fun () -> Gc.set old) f

(** {1 Statistics} *)

type stats = {
  mean_us : float;
  stddev_us : float;
  p50_us : float;
  p95_us : float;
  p99_us : float;
  min_us : float;
  max_us : float;
  qps : float;
  count : int;
}

let percentile sorted n p =
  if n = 1 then sorted.(0)
  else
    let idx = (float (n - 1)) *. p in
    let lo = int_of_float (floor idx) in
    let hi = min (n - 1) (lo + 1) in
    let frac = idx -. float lo in
    sorted.(lo) *. (1.0 -. frac) +. sorted.(hi) *. frac

let compute_stats latencies =
  let n = Array.length latencies in
  if n = 0 then
    { mean_us = 0.0; stddev_us = 0.0; p50_us = 0.0; p95_us = 0.0;
      p99_us = 0.0; min_us = 0.0; max_us = 0.0; qps = 0.0; count = 0 }
  else
    let sorted = Array.copy latencies in
    Array.sort Float.compare sorted;
    let total = Array.fold_left ( +. ) 0.0 sorted in
    let mean = total /. float n in
    let sum_sq = Array.fold_left (fun acc x ->
      let d = x -. mean in acc +. d *. d) 0.0 sorted in
    let stddev = if n > 1 then sqrt (sum_sq /. float (n - 1)) else 0.0 in
    {
      mean_us = mean;
      stddev_us = stddev;
      p50_us = percentile sorted n 0.50;
      p95_us = percentile sorted n 0.95;
      p99_us = percentile sorted n 0.99;
      min_us = sorted.(0);
      max_us = sorted.(n - 1);
      qps = if total > 0.0 then float n /. (total /. 1_000_000.0) else 0.0;
      count = n;
    }

let stats_to_json s : Yojson.Basic.t =
  `Assoc [
    ("mean_latency_us", `Float s.mean_us);
    ("stddev_us", `Float s.stddev_us);
    ("p50_latency_us", `Float s.p50_us);
    ("p95_latency_us", `Float s.p95_us);
    ("p99_latency_us", `Float s.p99_us);
    ("min_latency_us", `Float s.min_us);
    ("max_latency_us", `Float s.max_us);
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

(** {1 Dataset loading}

    Binary format (.fbin): int32_le n, int32_le dim, n*dim float32_le.
    Ground truth (.ibin): int32_le n_queries, int32_le k, n_queries*k int32_le. *)

let load_fbin path =
  let ic = open_in_bin path in
  Fun.protect ~finally:(fun () -> close_in ic) (fun () ->
    let buf4 = Bytes.create 4 in
    let read_int32 () =
      really_input ic buf4 0 4;
      Int32.to_int (Bytes.get_int32_le buf4 0)
    in
    let n = read_int32 () in
    let dim = read_int32 () in
    let row_buf = Bytes.create (dim * 4) in
    let vectors = Array.init n (fun _ ->
      really_input ic row_buf 0 (dim * 4);
      Array.init dim (fun j ->
        Int32.float_of_bits (Bytes.get_int32_le row_buf (j * 4)))
    ) in
    (vectors, dim))

let load_ibin path =
  let ic = open_in_bin path in
  Fun.protect ~finally:(fun () -> close_in ic) (fun () ->
    let buf4 = Bytes.create 4 in
    let read_int32 () =
      really_input ic buf4 0 4;
      Int32.to_int (Bytes.get_int32_le buf4 0)
    in
    let n = read_int32 () in
    let k = read_int32 () in
    let row_buf = Bytes.create (k * 4) in
    let gt = Array.init n (fun _ ->
      really_input ic row_buf 0 (k * 4);
      Array.init k (fun j ->
        Int32.to_int (Bytes.get_int32_le row_buf (j * 4)))
    ) in
    (gt, k))

(** {1 Memory measurement} *)

let parse_proc_status_field name =
  try
    let ic = open_in "/proc/self/status" in
    Fun.protect ~finally:(fun () -> close_in ic) (fun () ->
      let prefix = name ^ ":" in
      let plen = String.length prefix in
      let result = ref 0 in
      (try while true do
        let line = input_line ic in
        if String.length line > plen
           && String.sub line 0 plen = prefix then begin
          let s = String.trim (String.sub line plen (String.length line - plen)) in
          (match String.split_on_char ' ' s with
           | num :: _ -> result := int_of_string (String.trim num)
           | _ -> ());
          raise Exit
        end
      done with Exit | End_of_file -> ());
      !result)
  with _ -> 0

let get_rss_kb () = parse_proc_status_field "VmRSS"
let get_peak_rss_kb () = parse_proc_status_field "VmPeak"

(** {1 System metadata} *)

let system_metadata () : Yojson.Basic.t =
  let read_first_line cmd =
    try
      let ic = Unix.open_process_in cmd in
      Fun.protect ~finally:(fun () -> ignore (Unix.close_process_in ic))
        (fun () -> try input_line ic with End_of_file -> "unknown")
    with _ -> "unknown"
  in
  `Assoc [
    ("ocaml_version", `String Sys.ocaml_version);
    ("os", `String (read_first_line "uname -s -r"));
    ("cpu", `String (read_first_line
      "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs"));
    ("hostname", `String (read_first_line "hostname"));
    ("word_size", `Int Sys.int_size);
  ]

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

let has_flag name =
  let flag = "--" ^ name in
  Array.exists (fun s -> s = flag) Sys.argv

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

let hnsw_params_to_json (p : Gvecdb.Hnsw.params) : Yojson.Basic.t =
  `Assoc [
    ("m", `Int p.m);
    ("m_max", `Int p.m_max);
    ("ef_construction", `Int p.ef_construction);
    ("max_layers", `Int p.max_layers);
    ("ml", `Float p.ml);
  ]
