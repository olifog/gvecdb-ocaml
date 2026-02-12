type bigstring = Common.bigstring

let page_size = 4096
let max_supported_layers = 7
let layer0_max_neighbors = 32
let upper_layer_max_neighbors = 8

(* Node layout

   [layer_count : 1 byte] [padding : 7 bytes]
   [layer 0 neighbors : layer0_max_neighbors * 4 bytes]
   [layer 1..N neighbors : upper_layer_max_neighbors * 4 bytes each]
   [vector_id : 8 bytes] [vector_offset : 8 bytes] [deleted : 1 byte]
   [padding to node_size] *)

let node_layer0_off = 8
let layer0_size = layer0_max_neighbors * 4
let upper_layer_size = upper_layer_max_neighbors * 4

let layer_offset layer =
  if layer = 0 then node_layer0_off
  else node_layer0_off + layer0_size + (layer - 1) * upper_layer_size

let layer_neighbor_count layer =
  if layer = 0 then layer0_max_neighbors else upper_layer_max_neighbors

let metadata_offset =
  node_layer0_off + layer0_size + (max_supported_layers - 1) * upper_layer_size

let node_vector_id_off = metadata_offset
let node_vector_offset_off = metadata_offset + 8
let node_deleted_off = metadata_offset + 16
let node_data_end = node_deleted_off + 1
let nodes_per_page = page_size / node_data_end
let node_size = page_size / nodes_per_page
let node_layer_count_off = 0
let slot_to_page slot_id = slot_id / nodes_per_page
let slot_offset_in_page slot_id = slot_id mod nodes_per_page * node_size

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
  vector_offset : int64;
  deleted : bool;
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
            Bytes.get_int32_le page (offset + base + i * 4) |> Int32.to_int))
  in
  {
    layer_count;
    neighbors;
    vector_id = Bytes.get_int64_le page (offset + node_vector_id_off);
    vector_offset = Bytes.get_int64_le page (offset + node_vector_offset_off);
    deleted = Bytes.get page (offset + node_deleted_off) <> '\x00';
  }

let write_node_to_page (page : bytes) ~offset (n : node_data) =
  Bytes.set page (offset + node_layer_count_off) (Char.chr n.layer_count);
  for i = 1 to 7 do
    Bytes.set page (offset + i) '\x00'
  done;
  for layer = 0 to max_supported_layers - 1 do
    let base = layer_offset layer in
    let count = layer_neighbor_count layer in
    let actual =
      if layer < Array.length n.neighbors then n.neighbors.(layer)
      else [||]
    in
    for i = 0 to count - 1 do
      let v = if i < Array.length actual then actual.(i) else -1 in
      Bytes.set_int32_le page (offset + base + i * 4) (Int32.of_int v)
    done
  done;
  Bytes.set_int64_le page (offset + node_vector_id_off) n.vector_id;
  Bytes.set_int64_le page (offset + node_vector_offset_off) n.vector_offset;
  Bytes.set page
    (offset + node_deleted_off)
    (if n.deleted then '\x01' else '\x00');
  for i = node_deleted_off + 1 to node_size - 1 do
    Bytes.set page (offset + i) '\x00'
  done

let create_empty_page () = Bytes.make page_size '\x00'
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
            Bigstringaf.get_int32_le mmap (file_offset + base + i * 4)
            |> Int32.to_int))
  in
  {
    layer_count;
    neighbors;
    vector_id = Bigstringaf.get_int64_le mmap (file_offset + node_vector_id_off);
    vector_offset =
      Bigstringaf.get_int64_le mmap (file_offset + node_vector_offset_off);
    deleted = Bigstringaf.get mmap (file_offset + node_deleted_off) <> '\x00';
  }

let page_to_bigstring (page : bytes) =
  let bs = Bigstringaf.create (Bytes.length page) in
  Bigstringaf.blit_from_bytes page ~src_off:0 bs ~dst_off:0
    ~len:(Bytes.length page);
  bs

let mmap_to_bytes (mmap : bigstring) ~offset ~len =
  let b = Bytes.create len in
  Bigstringaf.blit_to_bytes mmap ~src_off:offset b ~dst_off:0 ~len;
  b

let mmap_layer_count (mmap : bigstring) ~file_offset =
  let lc = Char.code (Bigstringaf.get mmap (file_offset + node_layer_count_off)) in
  max 1 (min lc max_supported_layers)

let mmap_is_deleted (mmap : bigstring) ~file_offset =
  Bigstringaf.get mmap (file_offset + node_deleted_off) <> '\x00'

let mmap_vector_offset (mmap : bigstring) ~file_offset =
  Bigstringaf.get_int64_le mmap (file_offset + node_vector_offset_off)

let mmap_vector_id (mmap : bigstring) ~file_offset =
  Bigstringaf.get_int64_le mmap (file_offset + node_vector_id_off)

let iter_neighbors_mmap (mmap : bigstring) ~file_offset ~layer ~f =
  let base = layer_offset layer in
  let count = layer_neighbor_count layer in
  for i = 0 to count - 1 do
    let n = Bigstringaf.get_int32_le mmap (file_offset + base + i * 4) |> Int32.to_int in
    if n >= 0 then f n
  done
