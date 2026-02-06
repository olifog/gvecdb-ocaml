(** HNSW MVCC - Copy-on-Write persistence with snapshot isolation

    File layout:
    - Superblock (4KB): magic, version, params, active page table pointer
    - Page Table A (64KB): epoch, metadata, page offsets
    - Page Table B (64KB): shadow page table for atomic swap
    - Free List (4KB): recycled page offsets
    - Data Pages: append-only node pages (4KB each, 10 nodes per page)

    Write serialization is provided by the caller holding an LMDB write
    transaction (single-writer guarantee). Readers use epoch-based snapshot
    isolation via page table snapshots, independent of the write path. *)

type bigstring = Common.bigstring

let magic = "GVECHNSW"
let current_version = 2L
let superblock_size = 4096
let page_table_size = 65536 (* 64KB - fits ~8000 pages = 80K nodes *)
let free_list_size = 4096
let header_size = superblock_size + (2 * page_table_size) + free_list_size
let sb_magic_off = 0
let sb_version_off = 8
let sb_active_root_off = 16
let sb_m_off = 24
let sb_m_max_off = 28
let sb_ef_construction_off = 32
let sb_max_layers_off = 36
let sb_ml_off = 40
let sb_checksum_off = 48
let pt_epoch_off = 0
let pt_page_count_off = 8
let pt_entry_point_off = 16
let pt_max_level_off = 20
let pt_node_count_off = 24
let pt_dimension_off = 32
let pt_metric_off = 36
let pt_max_data_offset_off = 40 (* tracks actual end of data region *)
let pt_offsets_off = 48

let max_pages_in_table =
  (page_table_size - pt_offsets_off - 4) / 8 (* ~8000 pages *)

let fl_count_off = 0
let fl_entries_off = 8
let max_free_entries = (free_list_size - fl_entries_off) / 8

type error =
  | Invalid_magic
  | Version_mismatch of int64
  | File_too_small
  | IO_error of string
  | Checksum_error of string
  | Corrupted_data of string
  | Capacity_exceeded

let error_to_string = function
  | Invalid_magic -> "invalid magic"
  | Version_mismatch v ->
      Printf.sprintf "version mismatch: got %Ld, expected %Ld" v current_version
  | File_too_small -> "file too small"
  | IO_error msg -> Printf.sprintf "IO error: %s" msg
  | Checksum_error msg -> Printf.sprintf "checksum error: %s" msg
  | Corrupted_data msg -> Printf.sprintf "corrupted: %s" msg
  | Capacity_exceeded ->
      Printf.sprintf "capacity exceeded (max %d nodes)"
        (max_pages_in_table * Hnsw_page.nodes_per_page)

type page_table = {
  epoch : int64;
  page_count : int;
  entry_point : int;
  max_level : int;
  node_count : int;
  dimension : int;
  metric : Types.distance_metric;
  offsets : int64 array;
  max_data_offset : int64;
}

type write_txn = {
  base_epoch : int64;
  base_table : page_table;
  dirty_pages : (int, bytes) Hashtbl.t;
  mutable overlay : (int, Hnsw_page.node_data) Hashtbl.t option;
      (* uncommitted writes *)
  mutable new_entry_point : int;
  mutable new_max_level : int;
  mutable new_node_count : int;
  mutable new_dimension : int;
}

type t = {
  fd : Unix.file_descr;
  mutable mmap : bigstring;
  mutable file_size : int;
  mutable active_table : page_table;
  mutable active_which : int;
  epochs : (int64, int ref) Hashtbl.t;
  epochs_lock : Mutex.t;
  free_pages : int64 Queue.t;
  params : Hnsw.params;
  metric : Types.distance_metric;
  rng : Random.State.t;
}

let metric_to_int = function
  | Types.Euclidean -> 0
  | Types.Cosine -> 1
  | Types.DotProduct -> 2

let metric_of_int = function
  | 0 -> Types.Euclidean
  | 1 -> Types.Cosine
  | _ -> Types.DotProduct

let create_mmap fd size =
  Bigarray.(array1_of_genarray (Unix.map_file fd Char C_layout true [| size |]))

let page_table_offset which = superblock_size + (which * page_table_size)
let free_list_offset () = superblock_size + (2 * page_table_size)

let read_page_table mmap which =
  let base = page_table_offset which in
  let page_count =
    Bigstringaf.get_int64_le mmap (base + pt_page_count_off) |> Int64.to_int
  in
  let page_count = max 0 (min page_count max_pages_in_table) in
  {
    epoch = Bigstringaf.get_int64_le mmap (base + pt_epoch_off);
    page_count;
    entry_point =
      Bigstringaf.get_int32_le mmap (base + pt_entry_point_off) |> Int32.to_int;
    max_level =
      Bigstringaf.get_int32_le mmap (base + pt_max_level_off) |> Int32.to_int;
    node_count =
      Bigstringaf.get_int64_le mmap (base + pt_node_count_off) |> Int64.to_int;
    dimension =
      Bigstringaf.get_int32_le mmap (base + pt_dimension_off) |> Int32.to_int;
    metric =
      Bigstringaf.get_int32_le mmap (base + pt_metric_off)
      |> Int32.to_int |> metric_of_int;
    offsets =
      Array.init page_count (fun i ->
          Bigstringaf.get_int64_le mmap (base + pt_offsets_off + (i * 8)));
    max_data_offset =
      Bigstringaf.get_int64_le mmap (base + pt_max_data_offset_off);
  }

let write_page_table mmap which pt =
  let base = page_table_offset which in
  Bigstringaf.set_int64_le mmap (base + pt_epoch_off) pt.epoch;
  Bigstringaf.set_int64_le mmap (base + pt_page_count_off)
    (Int64.of_int pt.page_count);
  Bigstringaf.set_int32_le mmap
    (base + pt_entry_point_off)
    (Int32.of_int pt.entry_point);
  Bigstringaf.set_int32_le mmap (base + pt_max_level_off)
    (Int32.of_int pt.max_level);
  Bigstringaf.set_int64_le mmap (base + pt_node_count_off)
    (Int64.of_int pt.node_count);
  Bigstringaf.set_int32_le mmap (base + pt_dimension_off)
    (Int32.of_int pt.dimension);
  Bigstringaf.set_int32_le mmap (base + pt_metric_off)
    (Int32.of_int (metric_to_int pt.metric));
  Bigstringaf.set_int64_le mmap
    (base + pt_max_data_offset_off)
    pt.max_data_offset;
  for i = 0 to pt.page_count - 1 do
    Bigstringaf.set_int64_le mmap
      (base + pt_offsets_off + (i * 8))
      pt.offsets.(i)
  done;
  let checksum_off = base + pt_offsets_off + (pt.page_count * 8) in
  Bigstringaf.set_int32_le mmap checksum_off
    (Hnsw_page.crc32 mmap base (checksum_off - base))

let verify_page_table_checksum mmap which =
  let base = page_table_offset which in
  let page_count =
    Bigstringaf.get_int64_le mmap (base + pt_page_count_off) |> Int64.to_int
  in
  if page_count < 0 || page_count > max_pages_in_table then false
  else
    let checksum_off = base + pt_offsets_off + (page_count * 8) in
    Bigstringaf.get_int32_le mmap checksum_off
    = Hnsw_page.crc32 mmap base (checksum_off - base)

let load_free_list mmap =
  let base = free_list_offset () in
  let q = Queue.create () in
  let count =
    Bigstringaf.get_int64_le mmap (base + fl_count_off) |> Int64.to_int
  in
  let count = max 0 (min count max_free_entries) in
  for i = 0 to count - 1 do
    Queue.push
      (Bigstringaf.get_int64_le mmap (base + fl_entries_off + (i * 8)))
      q
  done;
  q

let save_free_list mmap q =
  let base = free_list_offset () in
  let count = min (Queue.length q) max_free_entries in
  Bigstringaf.set_int64_le mmap (base + fl_count_off) (Int64.of_int count);
  let i = ref 0 in
  Queue.iter
    (fun off ->
      if !i < count then
        Bigstringaf.set_int64_le mmap (base + fl_entries_off + (!i * 8)) off;
      incr i)
    q

let initial_file_size = header_size + (64 * Hnsw_page.page_size)

let grow_file t new_size =
  Msync.msync t.mmap;
  Unix.ftruncate t.fd new_size;
  t.mmap <- create_mmap t.fd new_size;
  t.file_size <- new_size

let max_supported_layers = 5

let create path ~metric ~(params : Hnsw.params) =
  if params.max_layers > max_supported_layers then
    Error
      (Corrupted_data
         (Printf.sprintf "max_layers %d exceeds maximum supported (%d)"
            params.max_layers max_supported_layers))
  else
    try
      let dir = Filename.dirname path in
      if dir <> "" && dir <> "." && not (Sys.file_exists dir) then
        Unix.mkdir dir 0o755;
      let fd = Unix.openfile path Unix.[ O_RDWR; O_CREAT; O_TRUNC ] 0o644 in
      Unix.ftruncate fd initial_file_size;
      let mmap = create_mmap fd initial_file_size in
      (* superblock *)
      for i = 0 to 7 do
        Bigstringaf.set mmap (sb_magic_off + i) (String.get magic i)
      done;
      Bigstringaf.set_int64_le mmap sb_version_off current_version;
      Bigstringaf.set_int64_le mmap sb_active_root_off 0L;
      Bigstringaf.set_int32_le mmap sb_m_off (Int32.of_int params.m);
      Bigstringaf.set_int32_le mmap sb_m_max_off (Int32.of_int params.m_max);
      Bigstringaf.set_int32_le mmap sb_ef_construction_off
        (Int32.of_int params.ef_construction);
      Bigstringaf.set_int32_le mmap sb_max_layers_off
        (Int32.of_int params.max_layers);
      Bigstringaf.set_int64_le mmap sb_ml_off (Int64.bits_of_float params.ml);
      Bigstringaf.set_int32_le mmap sb_checksum_off
        (Hnsw_page.crc32 mmap 0 sb_checksum_off);
      (* empty page tables *)
      let empty =
        {
          epoch = 1L;
          page_count = 0;
          entry_point = -1;
          max_level = -1;
          node_count = 0;
          dimension = -1;
          metric;
          offsets = [||];
          max_data_offset = Int64.of_int header_size;
        }
      in
      write_page_table mmap 0 empty;
      write_page_table mmap 1 empty;
      (* empty free list *)
      Bigstringaf.set_int64_le mmap (free_list_offset () + fl_count_off) 0L;
      Msync.msync mmap;
      Ok
        {
          fd;
          mmap;
          file_size = initial_file_size;
          active_table = empty;
          active_which = 0;
          epochs = Hashtbl.create 16;
          epochs_lock = Mutex.create ();
          free_pages = Queue.create ();
          params;
          metric;
          rng = Random.State.make_self_init ();
        }
    with Unix.Unix_error (e, fn, _) ->
      Error (IO_error (Printf.sprintf "%s: %s" fn (Unix.error_message e)))

let open_existing path =
  if not (Sys.file_exists path) then Error (IO_error "file not found")
  else
    try
      let fd = Unix.openfile path Unix.[ O_RDWR ] 0o644 in
      let result =
        let file_size = (Unix.fstat fd).st_size in
        if file_size < header_size then Error File_too_small
        else
          let mmap = create_mmap fd file_size in
          let read_magic () =
            String.init 8 (fun i -> Bigstringaf.get mmap (sb_magic_off + i))
          in
          if read_magic () <> magic then Error Invalid_magic
          else
            let version = Bigstringaf.get_int64_le mmap sb_version_off in
            if version <> current_version then Error (Version_mismatch version)
            else
              (* Superblock checksum may be stale after a crash between the
         page table pointer swap and the checksum write. Tolerate this
         as long as at least one page table has a valid checksum. *)
              let active_which =
                Bigstringaf.get_int64_le mmap sb_active_root_off |> Int64.to_int
              in
              let active_which =
                if active_which = 0 || active_which = 1 then active_which else 0
              in
              let valid_which =
                if verify_page_table_checksum mmap active_which then
                  active_which
                else if verify_page_table_checksum mmap (1 - active_which) then
                  1 - active_which
                else -1
              in
              if valid_which < 0 then Error (Checksum_error "page tables")
              else
                let active_table = read_page_table mmap valid_which in
                let max_layers_stored =
                  Bigstringaf.get_int32_le mmap sb_max_layers_off
                  |> Int32.to_int
                in
                if max_layers_stored > max_supported_layers then
                  Error
                    (Corrupted_data
                       (Printf.sprintf
                          "stored max_layers %d exceeds maximum supported (%d)"
                          max_layers_stored max_supported_layers))
                else
                  let params =
                    {
                      Hnsw.m =
                        Bigstringaf.get_int32_le mmap sb_m_off |> Int32.to_int;
                      m_max =
                        Bigstringaf.get_int32_le mmap sb_m_max_off
                        |> Int32.to_int;
                      ef_construction =
                        Bigstringaf.get_int32_le mmap sb_ef_construction_off
                        |> Int32.to_int;
                      max_layers = max_layers_stored;
                      ml =
                        Bigstringaf.get_int64_le mmap sb_ml_off
                        |> Int64.float_of_bits;
                    }
                  in
                  Ok
                    {
                      fd;
                      mmap;
                      file_size;
                      active_table;
                      active_which = valid_which;
                      epochs = Hashtbl.create 16;
                      epochs_lock = Mutex.create ();
                      free_pages = load_free_list mmap;
                      params;
                      metric = active_table.metric;
                      rng = Random.State.make_self_init ();
                    }
      in
      (match result with Error _ -> Unix.close fd | Ok _ -> ());
      result
    with Unix.Unix_error (e, fn, _) ->
      Error (IO_error (Printf.sprintf "%s: %s" fn (Unix.error_message e)))

let close t =
  Msync.msync t.mmap;
  Unix.close t.fd

let sync t = Msync.msync t.mmap

let begin_read t =
  Mutex.lock t.epochs_lock;
  let table = t.active_table in
  (match Hashtbl.find_opt t.epochs table.epoch with
  | Some c -> incr c
  | None -> Hashtbl.add t.epochs table.epoch (ref 1));
  Mutex.unlock t.epochs_lock;
  table

let end_read t table =
  Mutex.lock t.epochs_lock;
  (match Hashtbl.find_opt t.epochs table.epoch with
  | Some c ->
      decr c;
      if !c <= 0 then Hashtbl.remove t.epochs table.epoch
  | None -> ());
  Mutex.unlock t.epochs_lock

let read_node t table ~slot_id =
  if slot_id < 0 || slot_id >= table.node_count then None
  else
    let page_id = Hnsw_page.slot_to_page slot_id in
    if page_id >= table.page_count then None
    else
      let page_offset = table.offsets.(page_id) in
      let node_offset = Hnsw_page.slot_offset_in_page slot_id in
      let file_offset = Int64.to_int page_offset + node_offset in
      Some (Hnsw_page.read_node_from_mmap t.mmap ~file_offset)

let begin_write t =
  let table = t.active_table in
  {
    base_epoch = table.epoch;
    base_table = table;
    dirty_pages = Hashtbl.create 16;
    overlay = None;
    new_entry_point = table.entry_point;
    new_max_level = table.max_level;
    new_node_count = table.node_count;
    new_dimension = table.dimension;
  }

let get_or_copy_page t txn page_id =
  match Hashtbl.find_opt txn.dirty_pages page_id with
  | Some page -> page
  | None ->
      let page =
        if page_id < txn.base_table.page_count then
          Hnsw_page.mmap_to_bytes t.mmap
            ~offset:(Int64.to_int txn.base_table.offsets.(page_id))
            ~len:Hnsw_page.page_size
        else Hnsw_page.create_empty_page ()
      in
      Hashtbl.replace txn.dirty_pages page_id page;
      page

let write_node t txn ~slot_id node =
  let page_id = Hnsw_page.slot_to_page slot_id in
  let page = get_or_copy_page t txn page_id in
  Hnsw_page.write_node_to_page page
    ~offset:(Hnsw_page.slot_offset_in_page slot_id)
    node;
  if slot_id >= txn.new_node_count then txn.new_node_count <- slot_id + 1

let set_entry_point txn ~entry_point ~max_level =
  txn.new_entry_point <- entry_point;
  txn.new_max_level <- max_level

let set_dimension txn ~dimension = txn.new_dimension <- dimension

let commit t txn =
  try
    (* Flush overlay to dirty_pages *)
    (match txn.overlay with
    | None -> ()
    | Some ov ->
        Hashtbl.iter
          (fun slot_id node ->
            let page_id = Hnsw_page.slot_to_page slot_id in
            let page = get_or_copy_page t txn page_id in
            Hnsw_page.write_node_to_page page
              ~offset:(Hnsw_page.slot_offset_in_page slot_id)
              node)
          ov);
    let max_dirty_page =
      Hashtbl.fold (fun pid _ m -> max pid m) txn.dirty_pages (-1)
    in
    let new_page_count = max txn.base_table.page_count (max_dirty_page + 1) in
    if new_page_count > max_pages_in_table then Error Capacity_exceeded
    else begin
      let dirty_list =
        Hashtbl.fold (fun pid page acc -> (pid, page) :: acc) txn.dirty_pages []
        |> List.sort (fun (a, _) (b, _) -> compare a b)
      in
      (* allocate space: reuse free pages or append *)
      let write_offset = ref txn.base_table.max_data_offset in
      let new_offsets =
        Array.init new_page_count (fun pid ->
            if pid < txn.base_table.page_count then txn.base_table.offsets.(pid)
            else 0L)
      in
      let allocations =
        List.map
          (fun (pid, page) ->
            let off =
              if not (Queue.is_empty t.free_pages) then Queue.pop t.free_pages
              else begin
                let o = !write_offset in
                write_offset := Int64.add o (Int64.of_int Hnsw_page.page_size);
                o
              end
            in
            (* recycle old page if overwriting *)
            if pid < txn.base_table.page_count then
              Queue.push txn.base_table.offsets.(pid) t.free_pages;
            new_offsets.(pid) <- off;
            (off, page))
          dirty_list
      in
      let new_max_offset = !write_offset in
      (* grow file if needed *)
      let needed = Int64.to_int new_max_offset in
      if needed > t.file_size then
        grow_file t (max (t.file_size * 2) (needed + initial_file_size));
      (* write pages *)
      List.iter
        (fun (off, page) ->
          let bs = Hnsw_page.page_to_bigstring page in
          Bigstringaf.blit bs ~src_off:0 t.mmap ~dst_off:(Int64.to_int off)
            ~len:Hnsw_page.page_size)
        allocations;
      (* build new page table *)
      let new_table =
        {
          epoch = Int64.add txn.base_epoch 1L;
          page_count = new_page_count;
          entry_point = txn.new_entry_point;
          max_level = txn.new_max_level;
          node_count = txn.new_node_count;
          dimension = txn.new_dimension;
          metric = t.metric;
          offsets = new_offsets;
          max_data_offset = new_max_offset;
        }
      in
      let shadow = 1 - t.active_which in
      write_page_table t.mmap shadow new_table;
      save_free_list t.mmap t.free_pages;
      Msync.msync t.mmap;
      (* atomic swap *)
      Bigstringaf.set_int64_le t.mmap sb_active_root_off (Int64.of_int shadow);
      Bigstringaf.set_int32_le t.mmap sb_checksum_off
        (Hnsw_page.crc32 t.mmap 0 sb_checksum_off);
      Msync.msync t.mmap;
      t.active_table <- new_table;
      t.active_which <- shadow;
      Ok ()
    end
  with
  | Unix.Unix_error (e, fn, _) ->
      Error (IO_error (Printf.sprintf "%s: %s" fn (Unix.error_message e)))
  | Invalid_argument msg -> Error (IO_error msg)

let rollback _t _txn = ()

let gc_old_epochs t =
  Mutex.lock t.epochs_lock;
  let current = t.active_table.epoch in
  let old =
    Hashtbl.fold
      (fun e c acc -> if e < current && !c <= 0 then e :: acc else acc)
      t.epochs []
  in
  List.iter (Hashtbl.remove t.epochs) old;
  Mutex.unlock t.epochs_lock;
  List.length old

let get_metric t = t.metric
let get_params t = t.params
let get_node_count t = t.active_table.node_count
let get_entry_point t = t.active_table.entry_point
let get_max_level t = t.active_table.max_level
let get_dimension t = t.active_table.dimension
let get_epoch t = t.active_table.epoch

let write_nodes t nodes ~entry_point ~max_level ~dimension =
  let txn = begin_write t in
  List.iter (fun (slot_id, node) -> write_node t txn ~slot_id node) nodes;
  set_entry_point txn ~entry_point ~max_level;
  set_dimension txn ~dimension;
  commit t txn

let table_entry_point table = table.entry_point
let table_max_level table = table.max_level
let table_node_count table = table.node_count

(* Search context for MVCC-based search *)
type search_context = {
  mvcc : t;
  table : page_table;
  overlay : (int, Hnsw_page.node_data) Hashtbl.t option;
      (* uncommitted writes *)
  query_dist : int -> float;
}

let create_search_context mvcc table ~query_dist ~overlay =
  { mvcc; table; overlay; query_dist }

(* Read with overlay fallback for read-your-writes *)
let read_with_overlay t table overlay ~slot_id =
  match overlay with
  | Some ov -> (
      match Hashtbl.find_opt ov slot_id with
      | Some node -> Some node
      | None -> read_node t table ~slot_id)
  | None -> read_node t table ~slot_id

let read_node_with_overlay ctx ~slot_id =
  read_with_overlay ctx.mvcc ctx.table ctx.overlay ~slot_id

let rec take_n n acc = function
  | [] -> List.rev acc
  | _ when n <= 0 -> List.rev acc
  | x :: rest -> take_n (n - 1) (x :: acc) rest

(* Beam search on MVCC snapshot - single layer *)
let search_layer_mvcc ctx ~entry_points ~ef ~layer =
  let visited = Hashtbl.create (ef * 2) in
  let candidates = Heap.create Heap.Min in
  let results = Topk.create ef in
  (* Initialize with entry points *)
  List.iter
    (fun ep ->
      if not (Hashtbl.mem visited ep) then begin
        Hashtbl.add visited ep ();
        let dist = ctx.query_dist ep in
        if Float.is_finite dist then begin
          Heap.push candidates dist ep;
          Topk.insert results dist ep
        end
      end)
    entry_points;

  (* Expand candidates *)
  let rec expand () =
    match Heap.pop candidates with
    | None -> ()
    | Some (c_dist, c_slot) ->
        let worst = Topk.worst_dist results in
        if c_dist > worst && Topk.is_full results then ()
        else begin
          match read_node_with_overlay ctx ~slot_id:c_slot with
          | None -> expand ()
          | Some node ->
              if node.deleted then expand ()
              else begin
                if layer < node.layer_count then begin
                  let neighs = node.neighbors.(layer) in
                  Array.iter
                    (fun n ->
                      if n >= 0 && not (Hashtbl.mem visited n) then begin
                        Hashtbl.add visited n ();
                        match read_node_with_overlay ctx ~slot_id:n with
                        | None -> ()
                        | Some n_node ->
                            if not n_node.deleted then begin
                              let n_dist = ctx.query_dist n in
                              if Float.is_finite n_dist then begin
                                let worst' = Topk.worst_dist results in
                                if n_dist < worst' || not (Topk.is_full results)
                                then begin
                                  Heap.push candidates n_dist n;
                                  Topk.insert results n_dist n
                                end
                              end
                            end
                      end)
                    neighs
                end;
                expand ()
              end
        end
  in
  expand ();
  Topk.to_sorted_list results |> List.map (fun (dist, slot) -> (slot, dist))

(* Full HNSW search on MVCC snapshot *)
let search_mvcc ctx ~k ~ef =
  let table = ctx.table in
  if table.entry_point < 0 then []
  else begin
    let ep = ref [ table.entry_point ] in
    for layer = table.max_level downto 1 do
      match search_layer_mvcc ctx ~entry_points:!ep ~ef:1 ~layer with
      | [] -> ()
      | results -> ep := List.map fst results
    done;
    let ef' = max ef k in
    let results = search_layer_mvcc ctx ~entry_points:!ep ~ef:ef' ~layer:0 in
    take_n k [] results
  end

(* Simple neighbor selection - take closest M
   candidates is (slot_id, distance) list, returns list of slot_ids *)
let select_neighbors candidates m =
  let sorted =
    List.sort (fun (_, d1) (_, d2) -> Float.compare d1 d2) candidates
  in
  List.map fst (take_n m [] sorted)

(* Helper to read node from overlay first, then from mmap *)
let read_node_from_txn t (txn : write_txn) ~slot_id =
  read_with_overlay t txn.base_table txn.overlay ~slot_id

(* Insert into MVCC - runs algorithm against mmap + overlay
   compute_distance: takes vector_offset and returns distance to query *)
let insert_mvcc t txn ~vector_id ~vector_offset ~compute_distance
    ~compute_pairwise_distance ~dimension =
  let params = t.params in
  let r = Random.State.float t.rng 1.0 in
  let level = int_of_float (-.log (max r Float.epsilon) *. params.Hnsw.ml) in
  let level = min level (params.Hnsw.max_layers - 1) in
  let slot_id = txn.new_node_count in

  (* Check capacity *)
  let new_page_id = Hnsw_page.slot_to_page slot_id in
  if new_page_id >= max_pages_in_table then Error Capacity_exceeded
  else begin
    (* Track dimension *)
    let dim_ok =
      if txn.new_dimension < 0 then begin
        txn.new_dimension <- dimension;
        true
      end
      else txn.new_dimension = dimension
    in
    if not dim_ok then Error (Corrupted_data "dimension mismatch")
    else begin
      (* Create new node *)
      let neighbors =
        Array.init (level + 1) (fun layer ->
            let m = if layer = 0 then 2 * params.m else params.m_max in
            Array.make m (-1))
      in
      let new_node : Hnsw_page.node_data =
        {
          layer_count = level + 1;
          neighbors;
          vector_id;
          vector_offset;
          deleted = false;
        }
      in

      (* Add to overlay for read-your-writes *)
      let overlay =
        match txn.overlay with
        | Some ov -> ov
        | None ->
            let ov = Hashtbl.create 64 in
            txn.overlay <- Some ov;
            ov
      in
      Hashtbl.replace overlay slot_id new_node;
      txn.new_node_count <- slot_id + 1;

      (* First node becomes entry point *)
      if txn.new_entry_point < 0 then begin
        txn.new_entry_point <- slot_id;
        txn.new_max_level <- level;
        Ok slot_id
      end
      else begin
        let exception Neighbor_not_found in
        try
          (* Create query_dist that uses the overlay to find nodes *)
          let query_dist other_slot =
            match read_node_from_txn t txn ~slot_id:other_slot with
            | None -> infinity
            | Some node -> compute_distance node.vector_offset
          in

          (* Create search context with overlay *)
          let ctx =
            {
              mvcc = t;
              table = txn.base_table;
              overlay = Some overlay;
              query_dist;
            }
          in

          (* Top-down search to find insertion points *)
          let ep = ref [ txn.new_entry_point ] in
          let current_max = txn.new_max_level in

          for layer = current_max downto level + 1 do
            match search_layer_mvcc ctx ~entry_points:!ep ~ef:1 ~layer with
            | [] -> ()
            | results -> ep := List.map fst results
          done;

          (* Layer-by-layer insertion *)
          for layer = min level current_max downto 0 do
            let m = if layer = 0 then 2 * params.m else params.m_max in
            let candidates =
              search_layer_mvcc ctx ~entry_points:!ep ~ef:params.ef_construction
                ~layer
            in
            let selected = select_neighbors candidates m in

            (* Set neighbors for new node *)
            let node = Hashtbl.find overlay slot_id in
            List.iteri
              (fun i neighbor_slot ->
                if i < Array.length node.neighbors.(layer) then
                  node.neighbors.(layer).(i) <- neighbor_slot)
              selected;

            (* Add bidirectional links *)
            List.iter
              (fun neighbor_slot ->
                let neighbor =
                  match Hashtbl.find_opt overlay neighbor_slot with
                  | Some n -> n
                  | None -> (
                      match
                        read_node t txn.base_table ~slot_id:neighbor_slot
                      with
                      | Some n ->
                          (* Copy to overlay for modification *)
                          let copy : Hnsw_page.node_data =
                            {
                              layer_count = n.layer_count;
                              neighbors = Array.map Array.copy n.neighbors;
                              vector_id = n.vector_id;
                              vector_offset = n.vector_offset;
                              deleted = n.deleted;
                            }
                          in
                          Hashtbl.replace overlay neighbor_slot copy;
                          copy
                      | None -> raise Neighbor_not_found)
                in
                if layer < neighbor.layer_count then begin
                  let n_neighbors = neighbor.neighbors.(layer) in
                  (* Find empty slot *)
                  let empty_idx = ref (-1) in
                  for i = 0 to Array.length n_neighbors - 1 do
                    if n_neighbors.(i) < 0 && !empty_idx < 0 then empty_idx := i
                  done;
                  if !empty_idx >= 0 then n_neighbors.(!empty_idx) <- slot_id
                  else begin
                    (* Neighbor list full - prune using correct semantics:
                     compare dist(neighbor, new_node) against dist(neighbor, existing)
                     to decide whether new_node is a better connection for neighbor. *)
                    let neighbor_offset = neighbor.vector_offset in
                    let new_offset = vector_offset in
                    let dist_to_new =
                      compute_pairwise_distance neighbor_offset new_offset
                    in
                    let worst_idx = ref (-1) in
                    let worst_dist = ref neg_infinity in
                    for i = 0 to Array.length n_neighbors - 1 do
                      let n_slot = n_neighbors.(i) in
                      if n_slot >= 0 then begin
                        let d =
                          match read_node_from_txn t txn ~slot_id:n_slot with
                          | Some n ->
                              compute_pairwise_distance neighbor_offset
                                n.vector_offset
                          | None -> infinity
                        in
                        if d > !worst_dist then begin
                          worst_dist := d;
                          worst_idx := i
                        end
                      end
                    done;
                    if !worst_idx >= 0 && dist_to_new < !worst_dist then
                      n_neighbors.(!worst_idx) <- slot_id
                  end
                end)
              selected;

            match candidates with
            | [] -> ()
            | _ -> ep := List.map fst candidates
          done;

          (* Update entry point if new node is higher *)
          if level > current_max then begin
            txn.new_entry_point <- slot_id;
            txn.new_max_level <- level
          end;

          Ok slot_id
        with Neighbor_not_found ->
          Error (Corrupted_data "neighbor not found during insertion")
      end
    end
  end
