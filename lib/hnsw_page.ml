type bigstring = Common.bigstring

let page_size = 4096
let nodes_per_page = 10
let node_size = 392
let layer0_max_neighbors = 32
let layer_max_neighbors = 8
let node_layer_count_off = 0
let node_layer0_off = 8
let node_layer1_off = 136
let node_layer2_off = 168
let node_layer3_off = 200
let node_layer4_off = 232
let node_vector_id_off = 264
let node_vector_offset_off = 272
let node_deleted_off = 280
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
  let get_i32 off = Bytes.get_int32_le page (offset + off) |> Int32.to_int in
  let get_i64 off = Bytes.get_int64_le page (offset + off) in
  let layer_count =
    Char.code (Bytes.get page (offset + node_layer_count_off))
  in
  let layer_count = max 1 (min layer_count 5) in
  let neighbors =
    Array.init layer_count (fun layer ->
        let base, count =
          match layer with
          | 0 -> (node_layer0_off, layer0_max_neighbors)
          | 1 -> (node_layer1_off, layer_max_neighbors)
          | 2 -> (node_layer2_off, layer_max_neighbors)
          | 3 -> (node_layer3_off, layer_max_neighbors)
          | _ -> (node_layer4_off, layer_max_neighbors)
        in
        Array.init count (fun i -> get_i32 (base + (i * 4))))
  in
  {
    layer_count;
    neighbors;
    vector_id = get_i64 node_vector_id_off;
    vector_offset = get_i64 node_vector_offset_off;
    deleted = Bytes.get page (offset + node_deleted_off) <> '\x00';
  }

let write_node_to_page (page : bytes) ~offset (n : node_data) =
  Bytes.set page (offset + node_layer_count_off) (Char.chr n.layer_count);
  for i = 1 to 7 do
    Bytes.set page (offset + i) '\x00'
  done;
  let write_layer base count layer_idx =
    let actual =
      if layer_idx < Array.length n.neighbors then n.neighbors.(layer_idx)
      else [||]
    in
    for i = 0 to count - 1 do
      let v = if i < Array.length actual then actual.(i) else -1 in
      Bytes.set_int32_le page (offset + base + (i * 4)) (Int32.of_int v)
    done
  in
  write_layer node_layer0_off layer0_max_neighbors 0;
  write_layer node_layer1_off layer_max_neighbors 1;
  write_layer node_layer2_off layer_max_neighbors 2;
  write_layer node_layer3_off layer_max_neighbors 3;
  write_layer node_layer4_off layer_max_neighbors 4;
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
  let get_i32 off =
    Bigstringaf.get_int32_le mmap (file_offset + off) |> Int32.to_int
  in
  let get_i64 off = Bigstringaf.get_int64_le mmap (file_offset + off) in
  let layer_count =
    Char.code (Bigstringaf.get mmap (file_offset + node_layer_count_off))
  in
  let layer_count = max 1 (min layer_count 5) in
  let neighbors =
    Array.init layer_count (fun layer ->
        let base, count =
          match layer with
          | 0 -> (node_layer0_off, layer0_max_neighbors)
          | 1 -> (node_layer1_off, layer_max_neighbors)
          | 2 -> (node_layer2_off, layer_max_neighbors)
          | 3 -> (node_layer3_off, layer_max_neighbors)
          | _ -> (node_layer4_off, layer_max_neighbors)
        in
        Array.init count (fun i -> get_i32 (base + (i * 4))))
  in
  {
    layer_count;
    neighbors;
    vector_id = get_i64 node_vector_id_off;
    vector_offset = get_i64 node_vector_offset_off;
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
