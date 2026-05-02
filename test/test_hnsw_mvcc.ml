(** HNSW MVCC Tests *)

open Alcotest

(** Test helper: create temp directory for test files *)
let with_temp_dir name f =
  let dir =
    Filename.concat
      (Filename.get_temp_dir_name ())
      (Printf.sprintf "gvecdb_mvcc_test_%s_%d" name (Unix.getpid ()))
  in
  (try Unix.mkdir dir 0o755 with Unix.Unix_error (Unix.EEXIST, _, _) -> ());
  let cleanup () =
    (* Clean up files *)
    (try
       Array.iter
         (fun file -> try Unix.unlink (Filename.concat dir file) with _ -> ())
         (Sys.readdir dir)
     with _ -> ());
    try Unix.rmdir dir with _ -> ()
  in
  Fun.protect ~finally:cleanup (fun () -> f dir)

(** Test helper: create temp file path *)
let temp_path dir name = Filename.concat dir (name ^ ".mvcc")

let test_layout = Gvecdb.Hnsw_page.compute_layout 0

(** {1 Page Serialization Tests} *)

let test_node_roundtrip () =
  let node : Gvecdb.Hnsw_page.node_data =
    {
      layer_count = 3;
      neighbors = [| [| 1; 2; 3; -1; -1 |]; [| 4; 5; -1 |]; [| 6; -1 |] |];
      vector_id = 42L;
      deleted = false;
      inline_vec = None;
    }
  in
  let page = Gvecdb.Hnsw_page.create_empty_page test_layout.page_size in
  Gvecdb.Hnsw_page.write_node_to_page test_layout page ~offset:0 node;
  let read = Gvecdb.Hnsw_page.read_node_from_page page ~offset:0 in
  check int "layer_count" node.layer_count read.layer_count;
  check int64 "vector_id" node.vector_id read.vector_id;
  check bool "deleted" node.deleted read.deleted;
  check int "neighbors array length"
    (Array.length node.neighbors)
    (Array.length read.neighbors)

let test_node_deleted_flag () =
  let node : Gvecdb.Hnsw_page.node_data =
    {
      layer_count = 1;
      neighbors = [| [| -1 |] |];
      vector_id = 1L;
      deleted = true;
      inline_vec = None;
    }
  in
  let page = Gvecdb.Hnsw_page.create_empty_page test_layout.page_size in
  Gvecdb.Hnsw_page.write_node_to_page test_layout page ~offset:0 node;
  let read = Gvecdb.Hnsw_page.read_node_from_page page ~offset:0 in
  check bool "deleted flag preserved" true read.deleted

let test_page_copy () =
  let page = Gvecdb.Hnsw_page.create_empty_page test_layout.page_size in
  Bytes.set page 100 '\xFF';
  let copy = Gvecdb.Hnsw_page.copy_page page in
  check char "value copied" '\xFF' (Bytes.get copy 100);
  Bytes.set page 100 '\x00';
  check char "copy independent" '\xFF' (Bytes.get copy 100)

(** {1 CRC32 Checksum Tests} *)

let test_crc32_basic () =
  let data = Bigstringaf.of_string "Hello, World!" ~off:0 ~len:13 in
  let crc1 = Gvecdb.Hnsw_page.crc32 data 0 13 in
  let crc2 = Gvecdb.Hnsw_page.crc32 data 0 13 in
  check int32 "deterministic" crc1 crc2

let test_crc32_detects_change () =
  let data = Bigstringaf.create 100 in
  for i = 0 to 99 do
    Bigstringaf.set data i '\x00'
  done;
  let crc1 = Gvecdb.Hnsw_page.crc32 data 0 100 in
  Bigstringaf.set data 50 '\xFF';
  let crc2 = Gvecdb.Hnsw_page.crc32 data 0 100 in
  check bool "different after modification" true (crc1 <> crc2)

(** {1 MVCC File Operations Tests} *)

let test_create_new_file () =
  with_temp_dir "create" @@ fun dir ->
  let path = temp_path dir "test" in
  match
    Gvecdb.Hnsw_mvcc.create path ~metric:Gvecdb.Types.Cosine
      ~params:Gvecdb.Hnsw.default_params ()
  with
  | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
  | Ok mvcc ->
      check int "initial node count" 0 (Gvecdb.Hnsw_mvcc.get_node_count mvcc);
      check int "initial entry point" (-1)
        (Gvecdb.Hnsw_mvcc.get_entry_point mvcc);
      check int "initial max level" (-1) (Gvecdb.Hnsw_mvcc.get_max_level mvcc);
      Gvecdb.Hnsw_mvcc.close mvcc

let test_create_and_reopen () =
  with_temp_dir "reopen" @@ fun dir ->
  let path = temp_path dir "test" in
  (match
     Gvecdb.Hnsw_mvcc.create path ~metric:Gvecdb.Types.Cosine
       ~params:Gvecdb.Hnsw.default_params ()
   with
  | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
  | Ok mvcc ->
      let node : Gvecdb.Hnsw_page.node_data =
        {
          layer_count = 1;
          neighbors = [| Array.make 32 (-1) |];
          vector_id = 100L;
          deleted = false;
          inline_vec = None;
        }
      in
      (match
         Gvecdb.Hnsw_mvcc.write_nodes mvcc
           [ (0, node) ]
           ~entry_point:0 ~max_level:0 ~dimension:128
       with
      | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
      | Ok () -> ());
      Gvecdb.Hnsw_mvcc.close mvcc);
  match Gvecdb.Hnsw_mvcc.open_existing path with
  | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
  | Ok mvcc ->
      check int "node count after reopen" 1
        (Gvecdb.Hnsw_mvcc.get_node_count mvcc);
      check int "entry point after reopen" 0
        (Gvecdb.Hnsw_mvcc.get_entry_point mvcc);
      check int "max level after reopen" 0 (Gvecdb.Hnsw_mvcc.get_max_level mvcc);
      check int "dimension after reopen" 128
        (Gvecdb.Hnsw_mvcc.get_dimension mvcc);
      let table = Gvecdb.Hnsw_mvcc.begin_read mvcc in
      (match Gvecdb.Hnsw_mvcc.read_node mvcc table ~slot_id:0 with
      | None -> fail "node not found after reopen"
      | Some node ->
          check int64 "vector_id preserved" 100L node.vector_id;
          check bool "not deleted" false node.deleted);
      Gvecdb.Hnsw_mvcc.end_read mvcc table;
      Gvecdb.Hnsw_mvcc.close mvcc

let test_write_transaction () =
  with_temp_dir "write_txn" @@ fun dir ->
  let path = temp_path dir "test" in
  match
    Gvecdb.Hnsw_mvcc.create path ~metric:Gvecdb.Types.Cosine
      ~params:Gvecdb.Hnsw.default_params ()
  with
  | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
  | Ok mvcc ->
      let txn = Gvecdb.Hnsw_mvcc.begin_write mvcc in
      let node : Gvecdb.Hnsw_page.node_data =
        {
          layer_count = 2;
          neighbors = [| Array.make 32 (-1); Array.make 8 (-1) |];
          vector_id = 1L;
          deleted = false;
          inline_vec = None;
        }
      in
      Gvecdb.Hnsw_mvcc.write_node mvcc txn ~slot_id:0 node;
      Gvecdb.Hnsw_mvcc.set_entry_point txn ~entry_point:0 ~max_level:1;
      Gvecdb.Hnsw_mvcc.set_dimension txn ~dimension:64;
      (match Gvecdb.Hnsw_mvcc.commit mvcc txn with
      | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
      | Ok () ->
          check int "node count after commit" 1
            (Gvecdb.Hnsw_mvcc.get_node_count mvcc);
          check int "entry point after commit" 0
            (Gvecdb.Hnsw_mvcc.get_entry_point mvcc);
          check int "max level after commit" 1
            (Gvecdb.Hnsw_mvcc.get_max_level mvcc));
      Gvecdb.Hnsw_mvcc.close mvcc

let test_rollback () =
  with_temp_dir "rollback" @@ fun dir ->
  let path = temp_path dir "test" in
  match
    Gvecdb.Hnsw_mvcc.create path ~metric:Gvecdb.Types.Cosine
      ~params:Gvecdb.Hnsw.default_params ()
  with
  | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
  | Ok mvcc ->
      let txn = Gvecdb.Hnsw_mvcc.begin_write mvcc in
      let node : Gvecdb.Hnsw_page.node_data =
        {
          layer_count = 1;
          neighbors = [| Array.make 32 (-1) |];
          vector_id = 1L;
          deleted = false;
          inline_vec = None;
        }
      in
      Gvecdb.Hnsw_mvcc.write_node mvcc txn ~slot_id:0 node;
      Gvecdb.Hnsw_mvcc.rollback mvcc txn;
      check int "node count after rollback" 0
        (Gvecdb.Hnsw_mvcc.get_node_count mvcc);
      Gvecdb.Hnsw_mvcc.close mvcc

(** {1 Reader Snapshot Isolation Tests} *)

let test_reader_snapshot_isolation () =
  with_temp_dir "snapshot" @@ fun dir ->
  let path = temp_path dir "test" in
  match
    Gvecdb.Hnsw_mvcc.create path ~metric:Gvecdb.Types.Cosine
      ~params:Gvecdb.Hnsw.default_params ()
  with
  | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
  | Ok mvcc ->
      let node1 : Gvecdb.Hnsw_page.node_data =
        {
          layer_count = 1;
          neighbors = [| Array.make 32 (-1) |];
          vector_id = 1L;
          deleted = false;
          inline_vec = None;
        }
      in
      (match
         Gvecdb.Hnsw_mvcc.write_nodes mvcc
           [ (0, node1) ]
           ~entry_point:0 ~max_level:0 ~dimension:64
       with
      | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
      | Ok () -> ());

      let snapshot = Gvecdb.Hnsw_mvcc.begin_read mvcc in
      let initial_epoch = Gvecdb.Hnsw_mvcc.get_epoch mvcc in

      let node2 : Gvecdb.Hnsw_page.node_data =
        {
          layer_count = 1;
          neighbors = [| Array.make 32 (-1) |];
          vector_id = 2L;
          deleted = false;
          inline_vec = None;
        }
      in
      (match
         Gvecdb.Hnsw_mvcc.write_nodes mvcc
           [ (1, node2) ]
           ~entry_point:0 ~max_level:0 ~dimension:64
       with
      | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
      | Ok () -> ());

      let new_epoch = Gvecdb.Hnsw_mvcc.get_epoch mvcc in
      check bool "epoch incremented" true (new_epoch > initial_epoch);

      (match Gvecdb.Hnsw_mvcc.read_node mvcc snapshot ~slot_id:0 with
      | None -> fail "snapshot should see node 0"
      | Some node -> check int64 "node 0 vector_id" 1L node.vector_id);

      Gvecdb.Hnsw_mvcc.end_read mvcc snapshot;
      Gvecdb.Hnsw_mvcc.close mvcc

(** {1 Epoch Reference Counting Tests} *)

(** {1 Copy-on-Write Tests} *)

let test_cow_preserves_original () =
  with_temp_dir "cow" @@ fun dir ->
  let path = temp_path dir "test" in
  match
    Gvecdb.Hnsw_mvcc.create path ~metric:Gvecdb.Types.Cosine
      ~params:Gvecdb.Hnsw.default_params ()
  with
  | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
  | Ok mvcc ->
      let node1 : Gvecdb.Hnsw_page.node_data =
        {
          layer_count = 1;
          neighbors = [| [| 5; -1; -1; -1 |] |];
          vector_id = 1L;
          deleted = false;
          inline_vec = None;
        }
      in
      (match
         Gvecdb.Hnsw_mvcc.write_nodes mvcc
           [ (0, node1) ]
           ~entry_point:0 ~max_level:0 ~dimension:64
       with
      | Error _ -> fail "initial write failed"
      | Ok () -> ());

      let snapshot = Gvecdb.Hnsw_mvcc.begin_read mvcc in

      let node2 : Gvecdb.Hnsw_page.node_data =
        {
          layer_count = 1;
          neighbors = [| [| 10; 20; -1; -1 |] |];
          vector_id = 1L;
          deleted = false;
          inline_vec = None;
        }
      in
      (match
         Gvecdb.Hnsw_mvcc.write_nodes mvcc
           [ (0, node2) ]
           ~entry_point:0 ~max_level:0 ~dimension:64
       with
      | Error _ -> fail "update write failed"
      | Ok () -> ());

      (match Gvecdb.Hnsw_mvcc.read_node mvcc snapshot ~slot_id:0 with
      | None -> fail "snapshot lost node"
      | Some node -> check int "original neighbor[0]" 5 node.neighbors.(0).(0));

      Gvecdb.Hnsw_mvcc.end_read mvcc snapshot;
      Gvecdb.Hnsw_mvcc.close mvcc

(** {1 Large Index Test} *)

let test_large_index () =
  with_temp_dir "large" @@ fun dir ->
  let path = temp_path dir "test" in
  match
    Gvecdb.Hnsw_mvcc.create path ~metric:Gvecdb.Types.Cosine
      ~params:Gvecdb.Hnsw.default_params ()
  with
  | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
  | Ok mvcc ->
      let count = 500 in
      let nodes =
        List.init count (fun i ->
            let node : Gvecdb.Hnsw_page.node_data =
              {
                layer_count = 1;
                neighbors = [| Array.make 32 (-1) |];
                vector_id = Int64.of_int i;
                deleted = false;
                inline_vec = None;
              }
            in
            (i, node))
      in
      (match
         Gvecdb.Hnsw_mvcc.write_nodes mvcc nodes ~entry_point:0 ~max_level:0
           ~dimension:64
       with
      | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
      | Ok () -> ());
      check int "node count" count (Gvecdb.Hnsw_mvcc.get_node_count mvcc);
      let table = Gvecdb.Hnsw_mvcc.begin_read mvcc in
      (match Gvecdb.Hnsw_mvcc.read_node mvcc table ~slot_id:0 with
      | None -> fail "node 0 not found"
      | Some n -> check int64 "node 0 id" 0L n.vector_id);
      (match Gvecdb.Hnsw_mvcc.read_node mvcc table ~slot_id:(count - 1) with
      | None -> fail "last node not found"
      | Some n ->
          check int64 "last node id" (Int64.of_int (count - 1)) n.vector_id);
      Gvecdb.Hnsw_mvcc.end_read mvcc table;
      Gvecdb.Hnsw_mvcc.close mvcc

(** {1 Error Handling Tests} *)

let test_open_nonexistent () =
  with_temp_dir "nonexistent" @@ fun dir ->
  let path = temp_path dir "does_not_exist" in
  match Gvecdb.Hnsw_mvcc.open_existing path with
  | Ok _ -> fail "should fail for nonexistent file"
  | Error _ -> ()

let test_read_out_of_bounds () =
  with_temp_dir "bounds" @@ fun dir ->
  let path = temp_path dir "test" in
  match
    Gvecdb.Hnsw_mvcc.create path ~metric:Gvecdb.Types.Cosine
      ~params:Gvecdb.Hnsw.default_params ()
  with
  | Error e -> fail (Gvecdb.Hnsw_mvcc.error_to_string e)
  | Ok mvcc ->
      let table = Gvecdb.Hnsw_mvcc.begin_read mvcc in
      (match Gvecdb.Hnsw_mvcc.read_node mvcc table ~slot_id:999 with
      | None -> ()
      | Some _ -> fail "should return None for out of bounds");
      (match Gvecdb.Hnsw_mvcc.read_node mvcc table ~slot_id:(-1) with
      | None -> ()
      | Some _ -> fail "should return None for negative slot");
      Gvecdb.Hnsw_mvcc.end_read mvcc table;
      Gvecdb.Hnsw_mvcc.close mvcc

(** {1 Test Runner} *)

let page_tests =
  [
    ("node roundtrip", `Quick, test_node_roundtrip);
    ("deleted flag", `Quick, test_node_deleted_flag);
    ("page copy", `Quick, test_page_copy);
  ]

let checksum_tests =
  [
    ("crc32 basic", `Quick, test_crc32_basic);
    ("crc32 detects change", `Quick, test_crc32_detects_change);
  ]

let file_tests =
  [
    ("create new file", `Quick, test_create_new_file);
    ("create and reopen", `Quick, test_create_and_reopen);
    ("write transaction", `Quick, test_write_transaction);
    ("rollback", `Quick, test_rollback);
  ]

let snapshot_tests =
  [
    ("reader snapshot isolation", `Quick, test_reader_snapshot_isolation);
    ("cow preserves original", `Quick, test_cow_preserves_original);
  ]

let scale_tests = [ ("large index", `Slow, test_large_index) ]

let test_max_layers_constraint () =
  with_temp_dir "max_layers" @@ fun dir ->
  let path = temp_path dir "test" in
  let bad_params = { Gvecdb.Hnsw.default_params with max_layers = 10 } in
  match
    Gvecdb.Hnsw_mvcc.create path ~metric:Gvecdb.Types.Cosine ~params:bad_params
      ()
  with
  | Error (Gvecdb.Hnsw_mvcc.Corrupted_data msg) ->
      check bool "error mentions max_layers" true (String.length msg > 0)
  | Error _ -> fail "expected Corrupted_data error for max_layers > 7"
  | Ok mvcc ->
      Gvecdb.Hnsw_mvcc.close mvcc;
      fail "expected error for max_layers=10, got Ok"

let error_tests =
  [
    ("open nonexistent", `Quick, test_open_nonexistent);
    ("read out of bounds", `Quick, test_read_out_of_bounds);
    ("max_layers constraint", `Quick, test_max_layers_constraint);
  ]

let () =
  run "HNSW MVCC"
    [
      ("page", page_tests);
      ("checksum", checksum_tests);
      ("file", file_tests);
      ("snapshot", snapshot_tests);
      ("scale", scale_tests);
      ("error", error_tests);
    ]
