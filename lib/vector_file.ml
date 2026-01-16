(** append-only mmap-backed vector storage file

    file layout:
    - file header (32 bytes): magic, version, slot_size (reserved), next_offset
    - vector entries (32-byte aligned): 16-byte header + float32 data + padding

    vectors stored normalized (unit length) with original norm preserved for
    distance reconstruction *)

type bigstring = Common.bigstring

let magic = "GVECVECS"
let current_version = 3L
let alignment = 32

let file_header_size = 32
let file_header_magic_off = 0
let file_header_version_off = 8
let file_header_slot_size_off = 16
let file_header_next_offset_off = 24

let vec_header_size = 16
let vec_header_dim_off = 0
let vec_header_flags_off = 4
let vec_header_reserved_off = 5
let vec_header_norm_off = 8

let max_vector_dim = 1_000_000
let initial_file_size = 1024 * 1024

type vector_header = { dim : int; flags : int; norm : float }

let flag_normalized = 0x01
let is_normalized h = h.flags land flag_normalized <> 0

type t = {
  fd : Unix.file_descr;
  mutable mmap : bigstring;
  mutable file_size : int;
  path : string;
}

type error =
  | Invalid_magic
  | Version_mismatch of int64
  | File_too_small
  | Allocation_failed of string
  | IO_error of string
  | Invalid_offset of int64

let error_to_string = function
  | Invalid_magic -> "invalid magic number in vector file"
  | Version_mismatch v ->
      Printf.sprintf "vector file version mismatch: expected %Ld, got %Ld"
        current_version v
  | File_too_small -> "vector file too small to contain header"
  | Allocation_failed msg -> Printf.sprintf "vector allocation failed: %s" msg
  | IO_error msg -> Printf.sprintf "IO error: %s" msg
  | Invalid_offset off -> Printf.sprintf "invalid vector offset: %Ld" off

let align_up n =
  let mask = alignment - 1 in
  (n + mask) land lnot mask

let create_mmap fd size =
  let open Bigarray in
  let ba = Unix.map_file fd Char C_layout true [| size |] in
  array1_of_genarray ba

let get_magic mmap =
  let buf = Bytes.create 8 in
  for i = 0 to 7 do
    Bytes.set buf i (Bigstringaf.get mmap (file_header_magic_off + i))
  done;
  Bytes.to_string buf

let set_magic mmap =
  for i = 0 to 7 do
    Bigstringaf.set mmap (file_header_magic_off + i) (String.get magic i)
  done

let get_version mmap = Bigstringaf.get_int64_le mmap file_header_version_off
let set_version mmap v = Bigstringaf.set_int64_le mmap file_header_version_off v

let set_slot_size mmap s =
  Bigstringaf.set_int64_le mmap file_header_slot_size_off s

let get_next_offset mmap =
  Bigstringaf.get_int64_le mmap file_header_next_offset_off

let set_next_offset mmap offset =
  Bigstringaf.set_int64_le mmap file_header_next_offset_off offset

let validate_offset t offset =
  let off = Int64.to_int offset in
  if off < file_header_size || off + vec_header_size > t.file_size then
    Error (Invalid_offset offset)
  else Ok off

let read_header_at t offset : (vector_header, error) result =
  match validate_offset t offset with
  | Error e -> Error e
  | Ok off ->
      let dim =
        Int32.to_int
          (Bigstringaf.get_int32_le t.mmap (off + vec_header_dim_off))
      in
      let flags =
        Char.code (Bigstringaf.get t.mmap (off + vec_header_flags_off))
      in
      let norm =
        Int64.float_of_bits
          (Bigstringaf.get_int64_le t.mmap (off + vec_header_norm_off))
      in
      Ok { dim; flags; norm }

let write_header_at t off (h : vector_header) =
  Bigstringaf.set_int32_le t.mmap (off + vec_header_dim_off)
    (Int32.of_int h.dim);
  Bigstringaf.set t.mmap (off + vec_header_flags_off) (Char.chr h.flags);
  for i = 0 to 2 do
    Bigstringaf.set t.mmap (off + vec_header_reserved_off + i) '\x00'
  done;
  Bigstringaf.set_int64_le t.mmap
    (off + vec_header_norm_off)
    (Int64.bits_of_float h.norm)

let grow_file t new_size =
  Msync.msync t.mmap;
  Unix.ftruncate t.fd new_size;
  t.mmap <- create_mmap t.fd new_size;
  t.file_size <- new_size

let create path : (t, error) result =
  try
    let exists = Sys.file_exists path in
    let fd = Unix.openfile path [ Unix.O_RDWR; Unix.O_CREAT ] 0o644 in

    if not exists then begin
      Unix.ftruncate fd initial_file_size;
      let mmap = create_mmap fd initial_file_size in
      set_magic mmap;
      set_version mmap current_version;
      set_slot_size mmap 0L;
      set_next_offset mmap (Int64.of_int file_header_size);
      Msync.msync mmap;
      Ok { fd; mmap; file_size = initial_file_size; path }
    end
    else begin
      let stats = Unix.fstat fd in
      let file_size = stats.Unix.st_size in
      if file_size < file_header_size then begin
        Unix.close fd;
        Error File_too_small
      end
      else begin
        let mmap = create_mmap fd file_size in
        if get_magic mmap <> magic then begin
          Unix.close fd;
          Error Invalid_magic
        end
        else begin
          let version = get_version mmap in
          if version <> current_version then begin
            Unix.close fd;
            Error (Version_mismatch version)
          end
          else Ok { fd; mmap; file_size; path }
        end
      end
    end
  with Unix.Unix_error (err, fn, arg) ->
    Error
      (IO_error (Printf.sprintf "%s(%s): %s" fn arg (Unix.error_message err)))

let close t =
  Msync.msync t.mmap;
  Unix.close t.fd

let sync t = Msync.msync t.mmap

let vector_storage_size dim =
  if dim < 0 || dim > max_vector_dim then
    invalid_arg (Printf.sprintf "vector_storage_size: invalid dimension %d" dim);
  align_up (vec_header_size + (dim * 4))

let allocate t dim : (int64, error) result =
  let needed = vector_storage_size dim in
  let current_offset = Int64.to_int (get_next_offset t.mmap) in
  let new_offset = current_offset + needed in
  let grow_result =
    if new_offset > t.file_size then begin
      let new_size = max (t.file_size * 2) (new_offset + initial_file_size) in
      try
        grow_file t new_size;
        Ok ()
      with Unix.Unix_error (err, fn, arg) ->
        Error
          (Allocation_failed
             (Printf.sprintf "%s(%s): %s" fn arg (Unix.error_message err)))
    end
    else Ok ()
  in
  match grow_result with
  | Error e -> Error e
  | Ok () ->
      set_next_offset t.mmap (Int64.of_int new_offset);
      Ok (Int64.of_int current_offset)

let write_vector_at t offset ~normalized (data : bigstring) (norm : float) :
    (unit, error) result =
  match validate_offset t offset with
  | Error e -> Error e
  | Ok off ->
      let dim = Bigstringaf.length data / 4 in
      let needed = vec_header_size + Bigstringaf.length data in
      if off + needed > t.file_size then
        Error (Allocation_failed "write would exceed file bounds")
      else begin
        let flags = if normalized then flag_normalized else 0 in
        write_header_at t off { dim; flags; norm };
        Bigstringaf.blit data ~src_off:0 t.mmap ~dst_off:(off + vec_header_size)
          ~len:(Bigstringaf.length data);
        Ok ()
      end

let read_vector_at t offset : (bigstring, error) result =
  match read_header_at t offset with
  | Error e -> Error e
  | Ok header ->
      let off = Int64.to_int offset in
      let data_len = header.dim * 4 in
      if off + vec_header_size + data_len > t.file_size then
        Error (Invalid_offset offset)
      else
        Ok (Bigstringaf.sub t.mmap ~off:(off + vec_header_size) ~len:data_len)

let read_norm_at t offset : (float, error) result =
  match read_header_at t offset with
  | Error e -> Error e
  | Ok header -> Ok header.norm

let read_vector_with_header t offset : (bigstring * vector_header, error) result
    =
  match read_header_at t offset with
  | Error e -> Error e
  | Ok header ->
      let off = Int64.to_int offset in
      let data_len = header.dim * 4 in
      if off + vec_header_size + data_len > t.file_size then
        Error (Invalid_offset offset)
      else
        let data =
          Bigstringaf.sub t.mmap ~off:(off + vec_header_size) ~len:data_len
        in
        Ok (data, header)

let get_vector_dim_at t offset : (int, error) result =
  match read_header_at t offset with
  | Error e -> Error e
  | Ok header -> Ok header.dim

let get_allocated_bytes t = get_next_offset t.mmap
