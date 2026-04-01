(** Manual inspection test for HNSW MVCC file format.

    This test creates an index, populates it, and then manually reads the raw
    bytes to verify the file format is correct. *)

open Alcotest
module Bigstring = Bigstringaf

(* Constants from hnsw_mvcc.ml *)
let magic = "GVECHNSW"
let current_version = 3L
let superblock_size = 4096
let page_table_size = 4194304
let _header_size = superblock_size + (2 * page_table_size)

(* Superblock offsets *)
let sb_magic_off = 0
let sb_version_off = 8
let sb_active_root_off = 16
let sb_m_off = 24
let sb_m_max_off = 28
let sb_ef_construction_off = 32
let sb_max_layers_off = 36
let sb_ml_off = 40
let sb_checksum_off = 48

(* Page table offsets *)
let pt_epoch_off = 0
let pt_page_count_off = 8
let pt_entry_point_off = 16
let pt_max_level_off = 20
let pt_node_count_off = 24
let pt_dimension_off = 32
let pt_metric_off = 36
let pt_max_data_offset_off = 40
let pt_offsets_off = 48

(* Free list offsets *)

(* Node layout - derived from Hnsw_page *)
let page_size = Gvecdb.Hnsw_page.page_size
let nodes_per_page = Gvecdb.Hnsw_page.nodes_per_page
let node_size = Gvecdb.Hnsw_page.node_size
let node_layer_count_off = Gvecdb.Hnsw_page.node_layer_count_off
let node_layer0_off = Gvecdb.Hnsw_page.node_layer0_off
let node_vector_id_off = Gvecdb.Hnsw_page.node_vector_id_off
let node_vector_offset_off = Gvecdb.Hnsw_page.node_vector_offset_off
let node_deleted_off = Gvecdb.Hnsw_page.node_deleted_off
let layer0_max_neighbors = Gvecdb.Hnsw_page.layer0_max_neighbors
let page_table_offset which = superblock_size + (which * page_table_size)
(* CRC32 computation - must match hnsw_page.ml *)
let crc32_table =
  Array.init 256 (fun i ->
      let c = ref (Int32.of_int i) in
      for _ = 0 to 7 do
        c :=
          if Int32.(logand !c 1l <> 0l) then
            Int32.(logxor (shift_right_logical !c 1) 0xEDB88320l)
          else Int32.(shift_right_logical !c 1)
      done;
      !c)

let crc32 (data : bytes) offset len =
  let crc = ref 0xFFFFFFFFl in
  for i = 0 to len - 1 do
    let b = Char.code (Bytes.get data (offset + i)) in
    let idx = Int32.(to_int (logand (logxor !crc (of_int b)) 0xFFl)) in
    crc := Int32.(logxor (shift_right_logical !crc 8) crc32_table.(idx))
  done;
  Int32.logxor !crc 0xFFFFFFFFl

(* Helpers *)
let floats_to_bigstring (arr : float array) : Gvecdb.bigstring =
  let n = Array.length arr in
  let bs = Bigstring.create (n * 4) in
  for i = 0 to n - 1 do
    Bigstring.set_int32_le bs (i * 4) (Int32.bits_of_float arr.(i))
  done;
  bs

let random_vector dim = Array.init dim (fun _ -> Random.float 2.0 -. 1.0)

let ok_exn = function
  | Ok x -> x
  | Error e -> Alcotest.fail (Gvecdb.Error.to_string e)

let with_txn db f =
  match Gvecdb.with_transaction db f with
  | Some x -> x
  | None -> Alcotest.fail "transaction aborted"

(* Get temp path *)
let get_temp_path prefix =
  Filename.(
    concat (get_temp_dir_name ())
      (Printf.sprintf "%s_%d_%d" prefix (Unix.getpid ()) (Random.int 100000)))

let cleanup_db_files path =
  (try Sys.remove (path ^ ".db") with _ -> ());
  (try Sys.remove (path ^ ".vectors") with _ -> ());
  let hnsw_dir = path ^ ".hnsw" in
  try
    if Sys.file_exists hnsw_dir && Sys.is_directory hnsw_dir then begin
      Array.iter
        (fun f -> Sys.remove (Filename.concat hnsw_dir f))
        (Sys.readdir hnsw_dir);
      Unix.rmdir hnsw_dir
    end
  with _ -> ()

(* ============================================================================
   Binary Inspection Functions
   ============================================================================ *)

(* Inspection result - some fields are for display/debugging only *)
type inspection_result = {
  (* Superblock *)
  sb_magic : string;
  sb_version : int64;
  sb_active_which : int; [@warning "-69"]
  sb_m : int; [@warning "-69"]
  sb_m_max : int; [@warning "-69"]
  sb_ef_construction : int; [@warning "-69"]
  sb_max_layers : int; [@warning "-69"]
  sb_ml : float; [@warning "-69"]
  sb_checksum_valid : bool;
  (* Active Page Table *)
  pt_epoch : int64;
  pt_page_count : int;
  pt_entry_point : int;
  pt_max_level : int; [@warning "-69"]
  pt_node_count : int;
  pt_dimension : int;
  pt_metric : int; [@warning "-69"]
  pt_max_data_offset : int64; [@warning "-69"]
  pt_checksum_valid : bool;
  pt_page_offsets : int64 array;
  (* Shadow Page Table *)
  shadow_epoch : int64;
  shadow_node_count : int; [@warning "-69"]
  (* Sample Nodes *)
  sample_nodes : (int * node_inspection) list;
}

and node_inspection = {
  n_layer_count : int;
  n_vector_id : int64; [@warning "-69"]
  n_vector_offset : int64; [@warning "-69"]
  n_deleted : bool;
  n_layer0_neighbors : int list; (* First few valid neighbors *)
}

let read_file_bytes path =
  let ic = open_in_bin path in
  let len = in_channel_length ic in
  let buf = Bytes.create len in
  really_input ic buf 0 len;
  close_in ic;
  buf

let get_i32 buf off = Bytes.get_int32_le buf off |> Int32.to_int
let get_i64 buf off = Bytes.get_int64_le buf off

let inspect_mvcc_file path : inspection_result =
  let buf = read_file_bytes path in
  let file_size = Bytes.length buf in

  Printf.printf "\n=== MVCC File Inspection: %s ===\n" path;
  Printf.printf "File size: %d bytes (%.2f KB)\n\n" file_size
    (float_of_int file_size /. 1024.0);

  (* Superblock *)
  Printf.printf "--- Superblock (offset 0, size %d) ---\n" superblock_size;
  let sb_magic = Bytes.sub_string buf sb_magic_off 8 in
  let sb_version = get_i64 buf sb_version_off in
  let sb_active_which = get_i64 buf sb_active_root_off |> Int64.to_int in
  let sb_m = get_i32 buf sb_m_off in
  let sb_m_max = get_i32 buf sb_m_max_off in
  let sb_ef_construction = get_i32 buf sb_ef_construction_off in
  let sb_max_layers = get_i32 buf sb_max_layers_off in
  let sb_ml = Int64.float_of_bits (get_i64 buf sb_ml_off) in
  let sb_stored_csum = Bytes.get_int32_le buf sb_checksum_off in
  let sb_computed_csum = crc32 buf 0 sb_checksum_off in
  let sb_checksum_valid = sb_stored_csum = sb_computed_csum in

  Printf.printf "  magic: %S (expected %S) %s\n" sb_magic magic
    (if sb_magic = magic then "[OK]" else "[FAIL]");
  Printf.printf "  version: %Ld (expected %Ld) %s\n" sb_version current_version
    (if sb_version = current_version then "[OK]" else "[FAIL]");
  Printf.printf "  active_page_table: %d\n" sb_active_which;
  Printf.printf "  m: %d, m_max: %d, ef_construction: %d\n" sb_m sb_m_max
    sb_ef_construction;
  Printf.printf "  max_layers: %d, ml: %.4f\n" sb_max_layers sb_ml;
  Printf.printf "  checksum: stored=0x%08lX computed=0x%08lX %s\n"
    sb_stored_csum sb_computed_csum
    (if sb_checksum_valid then "[OK]" else "[FAIL]");

  (* Active Page Table *)
  let pt_base = page_table_offset sb_active_which in
  Printf.printf "\n--- Active Page Table %d (offset %d) ---\n" sb_active_which
    pt_base;

  let pt_epoch = get_i64 buf (pt_base + pt_epoch_off) in
  let pt_page_count =
    get_i64 buf (pt_base + pt_page_count_off) |> Int64.to_int
  in
  let pt_entry_point = get_i32 buf (pt_base + pt_entry_point_off) in
  let pt_max_level = get_i32 buf (pt_base + pt_max_level_off) in
  let pt_node_count =
    get_i64 buf (pt_base + pt_node_count_off) |> Int64.to_int
  in
  let pt_dimension = get_i32 buf (pt_base + pt_dimension_off) in
  let pt_metric = get_i32 buf (pt_base + pt_metric_off) in
  let pt_max_data_offset = get_i64 buf (pt_base + pt_max_data_offset_off) in

  Printf.printf "  epoch: %Ld\n" pt_epoch;
  Printf.printf "  page_count: %d\n" pt_page_count;
  Printf.printf "  entry_point: %d\n" pt_entry_point;
  Printf.printf "  max_level: %d\n" pt_max_level;
  Printf.printf "  node_count: %d\n" pt_node_count;
  Printf.printf "  dimension: %d\n" pt_dimension;
  Printf.printf "  metric: %d (%s)\n" pt_metric
    (match pt_metric with
    | 0 -> "Euclidean"
    | 1 -> "Cosine"
    | _ -> "DotProduct");
  Printf.printf "  max_data_offset: %Ld (%.2f KB)\n" pt_max_data_offset
    (Int64.to_float pt_max_data_offset /. 1024.0);

  (* Page offsets *)
  let pt_page_offsets =
    Array.init pt_page_count (fun i ->
        get_i64 buf (pt_base + pt_offsets_off + (i * 8)))
  in
  Printf.printf "  page_offsets: [";
  Array.iteri
    (fun i off ->
      if i < 5 || i >= pt_page_count - 2 then
        Printf.printf "%Ld%s" off (if i < pt_page_count - 1 then ", " else "")
      else if i = 5 then Printf.printf "..., ")
    pt_page_offsets;
  Printf.printf "]\n";

  (* Page table checksum *)
  let pt_checksum_off = pt_base + pt_offsets_off + (pt_page_count * 8) in
  let pt_stored_csum = Bytes.get_int32_le buf pt_checksum_off in
  let pt_computed_csum = crc32 buf pt_base (pt_checksum_off - pt_base) in
  let pt_checksum_valid = pt_stored_csum = pt_computed_csum in
  Printf.printf "  checksum: stored=0x%08lX computed=0x%08lX %s\n"
    pt_stored_csum pt_computed_csum
    (if pt_checksum_valid then "[OK]" else "[FAIL]");

  (* Shadow Page Table *)
  let shadow_which = 1 - sb_active_which in
  let shadow_base = page_table_offset shadow_which in
  let shadow_epoch = get_i64 buf (shadow_base + pt_epoch_off) in
  let shadow_node_count =
    get_i64 buf (shadow_base + pt_node_count_off) |> Int64.to_int
  in
  Printf.printf "\n--- Shadow Page Table %d (offset %d) ---\n" shadow_which
    shadow_base;
  Printf.printf "  epoch: %Ld\n" shadow_epoch;
  Printf.printf "  node_count: %d\n" shadow_node_count;

  (* Sample Nodes *)
  Printf.printf "\n--- Sample Nodes ---\n";
  let sample_slots =
    if pt_node_count <= 10 then List.init pt_node_count Fun.id
    else [ 0; 1; 2; pt_node_count / 2; pt_node_count - 2; pt_node_count - 1 ]
  in

  let sample_nodes =
    List.filter_map
      (fun slot_id ->
        if slot_id < 0 || slot_id >= pt_node_count then None
        else begin
          let page_id = slot_id / nodes_per_page in
          if page_id >= pt_page_count then None
          else begin
            let page_offset = Int64.to_int pt_page_offsets.(page_id) in
            let node_offset = slot_id mod nodes_per_page * node_size in
            let file_offset = page_offset + node_offset in

            if file_offset + node_size > file_size then None
            else begin
              let n_layer_count =
                Char.code (Bytes.get buf (file_offset + node_layer_count_off))
              in
              let n_vector_id =
                get_i64 buf (file_offset + node_vector_id_off)
              in
              let n_vector_offset =
                get_i64 buf (file_offset + node_vector_offset_off)
              in
              let n_deleted =
                Bytes.get buf (file_offset + node_deleted_off) <> '\x00'
              in

              (* Read layer 0 neighbors *)
              let n_layer0_neighbors =
                List.init (min 8 layer0_max_neighbors) (fun i ->
                    get_i32 buf (file_offset + node_layer0_off + (i * 4)))
                |> List.filter (fun n -> n >= 0)
              in

              Printf.printf "  slot %d (page %d, offset 0x%X):\n" slot_id
                page_id file_offset;
              Printf.printf "    layer_count: %d\n" n_layer_count;
              Printf.printf "    vector_id: %Ld\n" n_vector_id;
              Printf.printf "    vector_offset: %Ld\n" n_vector_offset;
              Printf.printf "    deleted: %b\n" n_deleted;
              Printf.printf "    layer0_neighbors (first 8 valid): %s\n"
                (String.concat ", " (List.map string_of_int n_layer0_neighbors));

              Some
                ( slot_id,
                  {
                    n_layer_count;
                    n_vector_id;
                    n_vector_offset;
                    n_deleted;
                    n_layer0_neighbors;
                  } )
            end
          end
        end)
      sample_slots
  in

  Printf.printf "\n";

  {
    sb_magic;
    sb_version;
    sb_active_which;
    sb_m;
    sb_m_max;
    sb_ef_construction;
    sb_max_layers;
    sb_ml;
    sb_checksum_valid;
    pt_epoch;
    pt_page_count;
    pt_entry_point;
    pt_max_level;
    pt_node_count;
    pt_dimension;
    pt_metric;
    pt_max_data_offset;
    pt_checksum_valid;
    pt_page_offsets;
    shadow_epoch;
    shadow_node_count;
    sample_nodes;
  }

(* ============================================================================
   Tests
   ============================================================================ *)

let test_create_and_inspect () =
  let base_path = get_temp_path "mvcc_inspect" in
  cleanup_db_files base_path;
  let db_path = base_path ^ ".db" in

  Printf.printf "\n\n========================================\n";
  Printf.printf "TEST: Create and Inspect Empty Index\n";
  Printf.printf "========================================\n";

  let db = ok_exn (Gvecdb.create db_path) in
  Gvecdb.close db;

  (* Find and inspect the HNSW file *)
  let hnsw_path = base_path ^ ".hnsw/embedding.hnsw.mvcc" in
  if not (Sys.file_exists hnsw_path) then
    Printf.printf "Note: No HNSW file yet (no vectors created)\n"
  else begin
    let result = inspect_mvcc_file hnsw_path in
    check string "magic" magic result.sb_magic;
    check int64 "version" current_version result.sb_version;
    check bool "sb checksum valid" true result.sb_checksum_valid
  end;

  cleanup_db_files base_path

let test_populate_and_inspect () =
  let base_path = get_temp_path "mvcc_populate" in
  cleanup_db_files base_path;
  let db_path = base_path ^ ".db" in
  let dim = 32 in
  let n_vectors = 200 in

  Printf.printf "\n\n========================================\n";
  Printf.printf "TEST: Populate %d Vectors and Inspect\n" n_vectors;
  Printf.printf "========================================\n";

  (* Create and populate *)
  let db = ok_exn (Gvecdb.create db_path) in
  let vector_ids =
    with_txn db (fun txn ->
        Array.init n_vectors (fun _ ->
            let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
            let vec = random_vector dim in
            ok_exn
              (Gvecdb.create_vector db ~txn Node node "embedding"
                 (floats_to_bigstring vec))))
  in

  Printf.printf "Created %d vectors\n" (Array.length vector_ids);

  (* Do a search to verify it works *)
  let query = random_vector dim in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:50 ~vector_tag:"embedding"
         query)
  in
  Printf.printf "Search returned %d results\n" (List.length results);

  Gvecdb.close db;

  (* Inspect the file *)
  let hnsw_path = base_path ^ ".hnsw/embedding.hnsw.mvcc" in
  check bool "HNSW file exists" true (Sys.file_exists hnsw_path);

  let result = inspect_mvcc_file hnsw_path in

  (* Verify structure *)
  check string "magic" magic result.sb_magic;
  check int64 "version" current_version result.sb_version;
  check bool "sb checksum valid" true result.sb_checksum_valid;
  check bool "pt checksum valid" true result.pt_checksum_valid;
  check int "node_count" n_vectors result.pt_node_count;
  check int "dimension" dim result.pt_dimension;
  check bool "entry_point valid" true (result.pt_entry_point >= 0);
  check bool "entry_point < node_count" true (result.pt_entry_point < n_vectors);

  (* Verify sample nodes *)
  List.iter
    (fun (slot_id, node) ->
      check bool
        (Printf.sprintf "slot %d layer_count > 0" slot_id)
        true (node.n_layer_count > 0);
      check bool
        (Printf.sprintf "slot %d not deleted" slot_id)
        false node.n_deleted;
      check bool
        (Printf.sprintf "slot %d has neighbors" slot_id)
        true
        (List.length node.n_layer0_neighbors > 0))
    result.sample_nodes;

  cleanup_db_files base_path

let test_delete_and_inspect () =
  let base_path = get_temp_path "mvcc_delete" in
  cleanup_db_files base_path;
  let db_path = base_path ^ ".db" in
  let dim = 16 in
  let n_vectors = 50 in
  let n_delete = 20 in

  Printf.printf "\n\n========================================\n";
  Printf.printf "TEST: Delete %d/%d Vectors and Inspect\n" n_delete n_vectors;
  Printf.printf "========================================\n";

  (* Create and populate *)
  let db = ok_exn (Gvecdb.create db_path) in
  let vector_ids =
    with_txn db (fun txn ->
        Array.init n_vectors (fun _ ->
            let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
            let vec = random_vector dim in
            ok_exn
              (Gvecdb.create_vector db ~txn Node node "embedding"
                 (floats_to_bigstring vec))))
  in

  Printf.printf "Created %d vectors\n" n_vectors;

  (* Delete some vectors *)
  with_txn db (fun txn ->
      for i = 0 to n_delete - 1 do
        ok_exn (Gvecdb.delete_vector db ~txn vector_ids.(i))
      done);

  Printf.printf "Deleted %d vectors\n" n_delete;

  Gvecdb.close db;

  (* Inspect *)
  let hnsw_path = base_path ^ ".hnsw/embedding.hnsw.mvcc" in
  let result = inspect_mvcc_file hnsw_path in

  (* After deletion, node_count stays the same but nodes are marked deleted *)
  check int "node_count unchanged" n_vectors result.pt_node_count;
  check bool "epoch > 1 (multiple commits)" true (result.pt_epoch > 1L);

  (* Count deleted nodes in sample *)
  let deleted_count =
    List.fold_left
      (fun acc (_, node) -> if node.n_deleted then acc + 1 else acc)
      0 result.sample_nodes
  in
  Printf.printf "Deleted nodes in sample: %d/%d\n" deleted_count
    (List.length result.sample_nodes);

  cleanup_db_files base_path

let test_persistence_and_inspect () =
  let base_path = get_temp_path "mvcc_persist" in
  cleanup_db_files base_path;
  let db_path = base_path ^ ".db" in
  let dim = 16 in
  let n_vectors = 100 in

  Printf.printf "\n\n========================================\n";
  Printf.printf "TEST: Persistence Round-Trip Inspection\n";
  Printf.printf "========================================\n";

  (* Phase 1: Create and close *)
  let db = ok_exn (Gvecdb.create db_path) in
  with_txn db (fun txn ->
      for _ = 0 to n_vectors - 1 do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let vec = random_vector dim in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node node "embedding"
               (floats_to_bigstring vec))
        in
        ()
      done);
  Gvecdb.close db;

  Printf.printf "Phase 1: Created %d vectors and closed\n" n_vectors;

  (* Inspect before reopen *)
  let hnsw_path = base_path ^ ".hnsw/embedding.hnsw.mvcc" in
  Printf.printf "\n--- Before Reopen ---\n";
  let result1 = inspect_mvcc_file hnsw_path in
  let epoch1 = result1.pt_epoch in

  (* Phase 2: Reopen and add more *)
  let db = ok_exn (Gvecdb.create db_path) in
  with_txn db (fun txn ->
      for _ = 0 to 49 do
        let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
        let vec = random_vector dim in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node node "embedding"
               (floats_to_bigstring vec))
        in
        ()
      done);
  Gvecdb.close db;

  Printf.printf "Phase 2: Added 50 more vectors and closed\n";

  (* Inspect after additions *)
  Printf.printf "\n--- After Additions ---\n";
  let result2 = inspect_mvcc_file hnsw_path in

  (* Verify *)
  check bool "epoch increased" true (result2.pt_epoch > epoch1);
  check int "node count increased" (n_vectors + 50) result2.pt_node_count;
  check bool "checksums still valid" true
    (result2.sb_checksum_valid && result2.pt_checksum_valid);

  cleanup_db_files base_path

let test_multi_epoch_inspection () =
  let base_path = get_temp_path "mvcc_epoch" in
  cleanup_db_files base_path;
  let db_path = base_path ^ ".db" in
  let dim = 8 in

  Printf.printf "\n\n========================================\n";
  Printf.printf "TEST: Multi-Epoch Commits Inspection\n";
  Printf.printf "========================================\n";

  let db = ok_exn (Gvecdb.create db_path) in

  (* Do multiple separate transactions to create multiple epochs *)
  for batch = 1 to 5 do
    with_txn db (fun txn ->
        for _ = 1 to 20 do
          let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
          let vec = random_vector dim in
          let _ =
            ok_exn
              (Gvecdb.create_vector db ~txn Node node "embedding"
                 (floats_to_bigstring vec))
          in
          ()
        done);
    Printf.printf "Batch %d: added 20 vectors\n" batch
  done;

  Gvecdb.close db;

  (* Inspect *)
  let hnsw_path = base_path ^ ".hnsw/embedding.hnsw.mvcc" in
  let result = inspect_mvcc_file hnsw_path in

  (* Should have epoch 6 (initial empty + 5 batches) *)
  Printf.printf "Final epoch: %Ld\n" result.pt_epoch;
  check bool "epoch >= 5" true (result.pt_epoch >= 5L);
  check int "total nodes" 100 result.pt_node_count;

  (* Active and shadow should differ *)
  Printf.printf "Active epoch: %Ld, Shadow epoch: %Ld\n" result.pt_epoch
    result.shadow_epoch;

  cleanup_db_files base_path

let test_large_scale_inspection () =
  let base_path = get_temp_path "mvcc_large" in
  cleanup_db_files base_path;
  let db_path = base_path ^ ".db" in
  let dim = 64 in
  let n_vectors = 1000 in

  Printf.printf "\n\n========================================\n";
  Printf.printf "TEST: Large Scale (%d vectors) Inspection\n" n_vectors;
  Printf.printf "========================================\n";

  let db = ok_exn (Gvecdb.create db_path) in

  (* Create in batches *)
  for batch = 0 to 9 do
    with_txn db (fun txn ->
        for _ = 0 to 99 do
          let node = ok_exn (Gvecdb.create_node db ~txn "doc") in
          let vec = random_vector dim in
          let _ =
            ok_exn
              (Gvecdb.create_vector db ~txn Node node "embedding"
                 (floats_to_bigstring vec))
          in
          ()
        done);
    Printf.printf "Batch %d: %d vectors total\n" (batch + 1) ((batch + 1) * 100)
  done;

  (* Test search *)
  let query = random_vector dim in
  let results =
    ok_exn
      (Gvecdb.knn_hnsw db ~metric:Cosine ~k:10 ~ef:100 ~vector_tag:"embedding"
         query)
  in
  Printf.printf "Search returned %d results\n" (List.length results);

  Gvecdb.close db;

  (* Inspect *)
  let hnsw_path = base_path ^ ".hnsw/embedding.hnsw.mvcc" in
  let result = inspect_mvcc_file hnsw_path in

  (* Verify structure *)
  check int "node count" n_vectors result.pt_node_count;
  check int "dimension" dim result.pt_dimension;

  (* Calculate expected pages *)
  let expected_pages = (n_vectors + nodes_per_page - 1) / nodes_per_page in
  Printf.printf "Expected pages: %d, Actual pages: %d\n" expected_pages
    result.pt_page_count;
  check bool "page count reasonable" true
    (result.pt_page_count >= expected_pages);

  (* Verify page offsets are sequential and non-overlapping *)
  let offsets_valid =
    let sorted = Array.copy result.pt_page_offsets in
    Array.sort Int64.compare sorted;
    let rec check_gaps i =
      if i >= Array.length sorted - 1 then true
      else
        let gap = Int64.sub sorted.(i + 1) sorted.(i) |> Int64.to_int in
        gap >= page_size && check_gaps (i + 1)
    in
    check_gaps 0 || Array.length sorted <= 1
  in
  check bool "page offsets non-overlapping" true offsets_valid;

  cleanup_db_files base_path

(* ============================================================================
   Test Runner
   ============================================================================ *)

let inspection_tests =
  [
    ("create_and_inspect", `Quick, test_create_and_inspect);
    ("populate_and_inspect", `Quick, test_populate_and_inspect);
    ("delete_and_inspect", `Quick, test_delete_and_inspect);
    ("persistence_and_inspect", `Quick, test_persistence_and_inspect);
    ("multi_epoch_inspection", `Quick, test_multi_epoch_inspection);
    ("large_scale_inspection", `Slow, test_large_scale_inspection);
  ]

let () = run "MVCC Inspection" [ ("inspection", inspection_tests) ]
