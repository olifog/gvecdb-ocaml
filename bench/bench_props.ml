(** Property Storage Benchmark

    Demonstrates zero-copy CapnProto reads: reading an integer field
    should have the same latency regardless of how large the backing
    blob is, because segments point directly into LMDB's mmap. *)

open Bench_common

module SchemaBuilder = Schemas.Make (Capnp.BytesMessage)
module SchemaReader = SchemaBuilder

let default_n = 5000

let find_capnp_path () =
  let candidates = [
    "test_schemas/schemas.capnp";
    "../test_schemas/schemas.capnp";
    "../../test_schemas/schemas.capnp";
  ] in
  match List.find_opt Sys.file_exists candidates with
  | Some p -> p
  | None -> failwith "cannot find test_schemas/schemas.capnp"

let register_schemas db =
  let path = find_capnp_path () in
  ignore (ok_exn (Gvecdb.register_schema_from_capnp db
      ~kind:Gvecdb.Schema_registry.NodeSchemaKind
      ~type_name:"person" ~capnp_path:path ~struct_name:"Person" ()))

(** Serialize a capnp builder message to a bigstring (wire format) *)
let capnp_to_bigstring to_message builder =
  let msg = to_message builder in
  let total = ref 0 in
  Capnp.Codecs.serialize_iter msg ~compression:`None ~f:(fun fragment ->
      total := !total + String.length fragment);
  let bs = Bigstringaf.create !total in
  let pos = ref 0 in
  Capnp.Codecs.serialize_iter msg ~compression:`None ~f:(fun fragment ->
      let len = String.length fragment in
      Bigstringaf.blit_from_string fragment ~src_off:0 bs ~dst_off:!pos ~len;
      pos := !pos + len);
  bs

(** Write N node properties with a given bio size *)
let bench_write ~n ~bio_size =
  Printf.printf "\n--- Property write (n=%d bio_size=%d) ---\n%!" n bio_size;
  let bio = String.make bio_size 'x' in
  with_bench_db (Printf.sprintf "props_w_%d" bio_size) @@ fun db _path ->
  register_schemas db;
  let latencies = Array.init n (fun i ->
    let node = ok_exn (Gvecdb.create_node db "person") in
    let ((), lat) = time_us (fun () ->
      let builder = SchemaBuilder.Builder.Person.init_root () in
      SchemaBuilder.Builder.Person.name_set builder "Alice Smith";
      SchemaBuilder.Builder.Person.age_set_int_exn builder 30;
      SchemaBuilder.Builder.Person.email_set builder "alice@example.com";
      SchemaBuilder.Builder.Person.bio_set builder bio;
      let bs = capnp_to_bigstring
          SchemaBuilder.Builder.Person.to_message builder in
      ok_exn (Gvecdb.set_node_props db node "person" bs)) in
    progress ~label:"write props" ~i ~n;
    lat
  ) in
  let stats = compute_stats latencies in
  Printf.printf "Write: mean=%.1fus p95=%.1fus (%.0f ops/s)\n%!"
    stats.mean_us stats.p95_us stats.qps;
  stats

(** Read only the integer age field -- should NOT scale with bio size *)
let bench_int_read ~n ~bio_size =
  Printf.printf "\n--- Zero-copy int read (n=%d bio_size=%d) ---\n%!" n bio_size;
  let bio = String.make bio_size 'x' in
  with_bench_db (Printf.sprintf "props_r_%d" bio_size) @@ fun db _path ->
  register_schemas db;
  let nodes = Array.init n (fun i ->
    let node = ok_exn (Gvecdb.create_node db "person") in
    let builder = SchemaBuilder.Builder.Person.init_root () in
    SchemaBuilder.Builder.Person.name_set builder "Alice Smith";
    SchemaBuilder.Builder.Person.age_set_int_exn builder 30;
    SchemaBuilder.Builder.Person.email_set builder "alice@example.com";
    SchemaBuilder.Builder.Person.bio_set builder bio;
    let bs = capnp_to_bigstring
        SchemaBuilder.Builder.Person.to_message builder in
    ok_exn (Gvecdb.set_node_props db node "person" bs);
    progress ~label:"setup nodes" ~i ~n;
    node
  ) in
  Gc.compact ();
  let rng = make_rng 999 in
  let latencies = Array.init (n * 2) (fun i ->
    let node = nodes.(Random.State.int rng n) in
    let (_, lat) = time_us (fun () ->
      let props_bs = ok_exn (Gvecdb.get_node_props db node) in
      let s = Bigstringaf.to_string props_bs in
      let stream = Capnp.Codecs.FramedStream.of_string ~compression:`None s in
      match Capnp.Codecs.FramedStream.get_next_frame stream with
      | Ok msg ->
          let ro_msg = Capnp.BytesMessage.Message.readonly msg in
          let reader = SchemaReader.Reader.Person.of_message ro_msg in
          ignore (SchemaReader.Reader.Person.age_get reader)
      | Error _ -> failwith "decode error") in
    progress ~label:"int read" ~i ~n:(n * 2);
    lat
  ) in
  let stats = compute_stats latencies in
  Printf.printf "Int read: mean=%.1fus p95=%.1fus (%.0f ops/s)\n%!"
    stats.mean_us stats.p95_us stats.qps;
  stats

(** Read all fields including text -- latency should scale with bio size *)
let bench_full_read ~n ~bio_size =
  Printf.printf "\n--- Full property read (n=%d bio_size=%d) ---\n%!" n bio_size;
  let bio = String.make bio_size 'x' in
  with_bench_db (Printf.sprintf "props_fr_%d" bio_size) @@ fun db _path ->
  register_schemas db;
  let nodes = Array.init n (fun i ->
    let node = ok_exn (Gvecdb.create_node db "person") in
    let builder = SchemaBuilder.Builder.Person.init_root () in
    SchemaBuilder.Builder.Person.name_set builder "Alice Smith";
    SchemaBuilder.Builder.Person.age_set_int_exn builder 30;
    SchemaBuilder.Builder.Person.email_set builder "alice@example.com";
    SchemaBuilder.Builder.Person.bio_set builder bio;
    let bs = capnp_to_bigstring
        SchemaBuilder.Builder.Person.to_message builder in
    ok_exn (Gvecdb.set_node_props db node "person" bs);
    progress ~label:"setup nodes" ~i ~n;
    node
  ) in
  Gc.compact ();
  let rng = make_rng 999 in
  let latencies = Array.init (n * 2) (fun i ->
    let node = nodes.(Random.State.int rng n) in
    let (_, lat) = time_us (fun () ->
      let props_bs = ok_exn (Gvecdb.get_node_props db node) in
      let s = Bigstringaf.to_string props_bs in
      let stream = Capnp.Codecs.FramedStream.of_string ~compression:`None s in
      match Capnp.Codecs.FramedStream.get_next_frame stream with
      | Ok msg ->
          let ro_msg = Capnp.BytesMessage.Message.readonly msg in
          let reader = SchemaReader.Reader.Person.of_message ro_msg in
          let _name = SchemaReader.Reader.Person.name_get reader in
          let _age = SchemaReader.Reader.Person.age_get reader in
          let _email = SchemaReader.Reader.Person.email_get reader in
          let _bio = SchemaReader.Reader.Person.bio_get reader in
          ()
      | Error _ -> failwith "decode error") in
    progress ~label:"full read" ~i ~n:(n * 2);
    lat
  ) in
  let stats = compute_stats latencies in
  Printf.printf "Full read: mean=%.1fus p95=%.1fus (%.0f ops/s)\n%!"
    stats.mean_us stats.p95_us stats.qps;
  stats

let () =
  let n = get_int_arg "n" default_n in
  let output_dir = get_string_arg "output" "bench_results" in
  ensure_output_dir output_dir;

  Printf.printf "=== Property storage benchmark (n=%d) ===\n%!" n;
  Printf.printf "Hypothesis: int-read latency is independent of bio size\n%!";

  let bio_sizes = [| 10; 1000; 100_000 |] in

  let write_results = Array.map (fun bio_size ->
    (bio_size, bench_write ~n ~bio_size)
  ) bio_sizes in

  let int_read_results = Array.map (fun bio_size ->
    (bio_size, bench_int_read ~n ~bio_size)
  ) bio_sizes in

  let full_read_results = Array.map (fun bio_size ->
    (bio_size, bench_full_read ~n ~bio_size)
  ) bio_sizes in

  (* Summary *)
  Printf.printf "\n=== Summary: int-read latency vs bio size ===\n%!";
  Array.iter (fun (bio_size, stats) ->
    Printf.printf "  bio=%6d: mean=%.1fus p95=%.1fus\n%!"
      bio_size stats.mean_us stats.p95_us
  ) int_read_results;

  Printf.printf "\n=== Summary: full-read latency vs bio size ===\n%!";
  Array.iter (fun (bio_size, stats) ->
    Printf.printf "  bio=%6d: mean=%.1fus p95=%.1fus\n%!"
      bio_size stats.mean_us stats.p95_us
  ) full_read_results;

  (* Output JSON *)
  let ts = timestamp () in
  let size_results_to_json results =
    `List (Array.to_list (Array.map (fun (bio_size, stats) ->
      `Assoc [
        ("bio_size", `Int bio_size);
        ("stats", stats_to_json stats);
      ]) results))
  in
  let json : Yojson.Basic.t = `Assoc [
    ("benchmark", `String "property_storage");
    ("timestamp", `String ts);
    ("params", `Assoc [("n", `Int n)]);
    ("write", size_results_to_json write_results);
    ("zerocopy_int_read", size_results_to_json int_read_results);
    ("full_read", size_results_to_json full_read_results);
  ] in
  let filename = Filename.concat output_dir
    (Printf.sprintf "props_%d_%s.json" n ts) in
  output_json ~filename json;

  Printf.printf "\nDone.\n%!"
