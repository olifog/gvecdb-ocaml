type bigstring = Common.bigstring

let default_page_size = 4096
let max_supported_layers = 7
let layer0_max_neighbors = 32
let upper_layer_max_neighbors = 8

(* Node layout:

   [layer_count : 1 byte] [padding : 7 bytes]
   [layer 0 neighbors : layer0_max_neighbors * 4 bytes]
   [layer 1..N neighbors : upper_layer_max_neighbors * 4 bytes each]
   [vector_id : 8 bytes] [deleted : 1 byte] [padding to alignment]
   [vec_header : 16 bytes (dim:4, flags:1, reserved:3, norm:8)]
   [vec_data : dim * 4 bytes, 32-byte aligned]
   [padding to node_size]

   node_vec_data_off is derived to land on a 32-byte boundary so that
   SIMD-aligned loads work when node_start is itself 32-byte aligned. *)

let node_layer_count_off = 0
let node_layer0_off = 8
let layer0_size = layer0_max_neighbors * 4
let upper_layer_size = upper_layer_max_neighbors * 4

let layer_offset layer =
  if layer = 0 then node_layer0_off
  else node_layer0_off + layer0_size + ((layer - 1) * upper_layer_size)

let layer_neighbor_count layer =
  if layer = 0 then layer0_max_neighbors else upper_layer_max_neighbors

let metadata_offset =
  node_layer0_off + layer0_size + ((max_supported_layers - 1) * upper_layer_size)

let node_vector_id_off = metadata_offset
let node_deleted_off = metadata_offset + 8
let node_data_end = node_deleted_off + 1
let vec_header_size = 16
let node_vec_data_off = (node_data_end + vec_header_size + 31) land lnot 31
let node_vec_header_off = node_vec_data_off - vec_header_size

type layout = {
  dim : int;
  node_size : int;
  page_size : int;
  nodes_per_page : int;
}

let max_dim = 1_000_000

let compute_layout dim =
  let d = max 0 (min dim max_dim) in
  let raw = node_vec_data_off + (d * 4) in
  let node_size = (raw + 31) land lnot 31 in
  let rec next_pow2 n = if n >= node_size then n else next_pow2 (n * 2) in
  let page_size = next_pow2 default_page_size in
  let nodes_per_page = max 1 (page_size / node_size) in
  { dim = d; node_size; page_size; nodes_per_page }

let slot_to_page layout slot_id = slot_id / layout.nodes_per_page

let slot_offset_in_page layout slot_id =
  slot_id mod layout.nodes_per_page * layout.node_size

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

let crc32 (data : bigstring) offset len =
  let crc = ref 0xFFFFFFFFl in
  for i = 0 to len - 1 do
    let b = Char.code (Bigstringaf.get data (offset + i)) in
    let idx = Int32.(to_int (logand (logxor !crc (of_int b)) 0xFFl)) in
    crc := Int32.(logxor (shift_right_logical !crc 8) crc32_table.(idx))
  done;
  Int32.logxor !crc 0xFFFFFFFFl

type node_data = {
  layer_count : int;
  neighbors : int array array;
  vector_id : int64;
  deleted : bool;
  inline_vec : bigstring option;
}

let read_node_from_page (page : bytes) ~offset : node_data =
  let layer_count =
    Char.code (Bytes.get page (offset + node_layer_count_off))
  in
  let layer_count = max 1 (min layer_count max_supported_layers) in
  let neighbors =
    Array.init layer_count (fun layer ->
        let base = layer_offset layer in
        let count = layer_neighbor_count layer in
        Array.init count (fun i ->
            Bytes.get_int32_le page (offset + base + (i * 4)) |> Int32.to_int))
  in
  {
    layer_count;
    neighbors;
    vector_id = Bytes.get_int64_le page (offset + node_vector_id_off);
    deleted = Bytes.get page (offset + node_deleted_off) <> '\x00';
    inline_vec = None;
  }

let write_node_to_page (layout : layout) (page : bytes) ~offset (n : node_data)
    =
  Bytes.set page (offset + node_layer_count_off) (Char.chr n.layer_count);
  for i = 1 to 7 do
    Bytes.set page (offset + i) '\x00'
  done;
  for layer = 0 to max_supported_layers - 1 do
    let base = layer_offset layer in
    let count = layer_neighbor_count layer in
    let actual =
      if layer < Array.length n.neighbors then n.neighbors.(layer) else [||]
    in
    for i = 0 to count - 1 do
      let v = if i < Array.length actual then actual.(i) else -1 in
      Bytes.set_int32_le page (offset + base + (i * 4)) (Int32.of_int v)
    done
  done;
  Bytes.set_int64_le page (offset + node_vector_id_off) n.vector_id;
  Bytes.set page
    (offset + node_deleted_off)
    (if n.deleted then '\x01' else '\x00');
  for i = node_deleted_off + 1 to node_vec_header_off - 1 do
    Bytes.set page (offset + i) '\x00'
  done;
  match n.inline_vec with
  | Some ivec ->
      let ivec_len = Bigstringaf.length ivec in
      assert (ivec_len <= layout.node_size - node_vec_header_off);
      Bigstringaf.blit_to_bytes ivec ~src_off:0 page
        ~dst_off:(offset + node_vec_header_off)
        ~len:ivec_len;
      for i = node_vec_header_off + ivec_len to layout.node_size - 1 do
        Bytes.set page (offset + i) '\x00'
      done
  | None ->
      (* CoW pages already contain the previous inline vec from the mmap copy,
         new pages are zero-filled. either way leaving the region untouched
         is correct since callers that modify only metadata/neighbors pass None. *)
      ()

let create_empty_page page_size = Bytes.make page_size '\x00'
let copy_page src = Bytes.copy src

let read_node_from_mmap (mmap : bigstring) ~file_offset : node_data =
  let layer_count =
    Char.code (Bigstringaf.get mmap (file_offset + node_layer_count_off))
  in
  let layer_count = max 1 (min layer_count max_supported_layers) in
  let neighbors =
    Array.init layer_count (fun layer ->
        let base = layer_offset layer in
        let count = layer_neighbor_count layer in
        Array.init count (fun i ->
            Bigstringaf.get_int32_le mmap (file_offset + base + (i * 4))
            |> Int32.to_int))
  in
  {
    layer_count;
    neighbors;
    vector_id = Bigstringaf.get_int64_le mmap (file_offset + node_vector_id_off);
    deleted = Bigstringaf.get mmap (file_offset + node_deleted_off) <> '\x00';
    inline_vec = None;
  }

let blit_page_to_mmap (page : bytes) (mmap : bigstring) ~dst_off ~len =
  Bigstringaf.blit_from_bytes page ~src_off:0 mmap ~dst_off ~len

let mmap_to_bytes (mmap : bigstring) ~offset ~len =
  let b = Bytes.create len in
  Bigstringaf.blit_to_bytes mmap ~src_off:offset b ~dst_off:0 ~len;
  b

let mmap_layer_count (mmap : bigstring) ~file_offset =
  let lc =
    Char.code (Bigstringaf.get mmap (file_offset + node_layer_count_off))
  in
  max 1 (min lc max_supported_layers)

let mmap_is_deleted (mmap : bigstring) ~file_offset =
  Bigstringaf.get mmap (file_offset + node_deleted_off) <> '\x00'

let iter_neighbors_mmap (mmap : bigstring) ~file_offset ~layer ~f =
  let base = layer_offset layer in
  let count = layer_neighbor_count layer in
  for i = 0 to count - 1 do
    let n =
      Bigstringaf.get_int32_le mmap (file_offset + base + (i * 4))
      |> Int32.to_int
    in
    if n >= 0 then f n
  done
