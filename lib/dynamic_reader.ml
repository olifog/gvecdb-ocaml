open Types
module Bigstring = Bigstringaf

type field_value =
  | V_void
  | V_bool of bool
  | V_int8 of int
  | V_int16 of int
  | V_int32 of int32
  | V_int64 of int64
  | V_uint8 of int
  | V_uint16 of int
  | V_uint32 of int32
  | V_uint64 of int64
  | V_float32 of float
  | V_float64 of float
  | V_text of string
  | V_data of string

let field_value_to_string = function
  | V_void -> "void"
  | V_bool b -> string_of_bool b
  | V_int8 v -> string_of_int v
  | V_int16 v -> string_of_int v
  | V_int32 v -> Int32.to_string v
  | V_int64 v -> Int64.to_string v
  | V_uint8 v -> string_of_int v
  | V_uint16 v -> string_of_int v
  | V_uint32 v -> Printf.sprintf "%ld" v
  | V_uint64 v -> Printf.sprintf "%Ld" v
  | V_float32 v -> string_of_float v
  | V_float64 v -> string_of_float v
  | V_text s -> s
  | V_data s -> Printf.sprintf "<data:%d>" (String.length s)

let parse_struct_sections (bs : bigstring) :
    (int * int * int * int, error) result =
  let len = Bigstring.length bs in
  if len < 16 then
    Error (Corrupted_data "blob too small for capnp message")
  else
    let segment_count = 1 + Int32.to_int (Bigstring.get_int32_le bs 0) in
    if segment_count < 1 || segment_count > 512 then
      Error (Corrupted_data "invalid segment count")
    else
      let header_words = (segment_count + 2) / 2 in
      let header_bytes = header_words * 8 in
      if header_bytes + 8 > len then
        Error (Corrupted_data "header exceeds blob size")
      else
        let root_ptr = Bigstring.get_int64_le bs header_bytes in
        (* bits[31:2] as a 30-bit signed offset in words *)
        let low32 = Int64.to_int (Int64.logand root_ptr 0xFFFF_FFFFL) in
        let raw = (low32 asr 2) land 0x3FFFFFFF in
        let ptr_offset_words =
          if raw land 0x20000000 <> 0 then raw lor (-0x40000000) else raw
        in
        let data_words = Int64.to_int
            (Int64.logand (Int64.shift_right_logical root_ptr 32) 0xFFFF_L) in
        let ptr_count = Int64.to_int
            (Int64.logand (Int64.shift_right_logical root_ptr 48) 0xFFFF_L) in
        let data_start = header_bytes + 8 + (ptr_offset_words * 8) in
        let ptr_start = data_start + (data_words * 8) in
        if data_start < 0 || ptr_start < 0 || ptr_start > len then
          Error (Corrupted_data "struct sections exceed blob bounds")
        else
          Ok (data_start, data_words, ptr_start, ptr_count)

(* follow a capnp list pointer to read byte-element list data
   subtract_nul: true for Text (has NUL terminator), false for Data *)
let read_list_pointer (bs : bigstring) (ptr_base : int) (ptr_index : int)
    ~subtract_nul : string =
  let ptr_offset = ptr_base + (ptr_index * 8) in
  if ptr_offset < 0 || ptr_offset + 8 > Bigstring.length bs then ""
  else
    let ptr_word = Bigstring.get_int64_le bs ptr_offset in
    if ptr_word = 0L then ""
    else
      let low32 = Int64.to_int (Int64.logand ptr_word 0xFFFF_FFFFL) in
      let raw = (low32 asr 2) land 0x3FFFFFFF in
      let offset_words =
        if raw land 0x20000000 <> 0 then raw lor (-0x40000000) else raw
      in
      let elem_count = Int64.to_int (Int64.shift_right_logical ptr_word 35) in
      let list_start = ptr_offset + 8 + (offset_words * 8) in
      let data_len = if subtract_nul then max 0 (elem_count - 1) else elem_count in
      if list_start < 0 || list_start + data_len > Bigstring.length bs then ""
      else Bigstring.substring bs ~off:list_start ~len:data_len

let read_field (bs : bigstring) ~data_start ~ptr_start
    (fd : Schema_registry.field_descriptor) : field_value =
  let open Schema_registry in
  if fd.is_pointer then
    let ptr_index = fd.offset / 8 in
    match fd.field_type with
    | Text -> V_text (read_list_pointer bs ptr_start ptr_index ~subtract_nul:true)
    | Data -> V_data (read_list_pointer bs ptr_start ptr_index ~subtract_nul:false)
    | _ -> V_void
  else
    match fd.field_type with
    | Void -> V_void
    | Bool ->
        (* offset is the raw capnp bit index *)
        let byte_off = data_start + (fd.offset / 8) in
        let bit_off = fd.offset mod 8 in
        if byte_off >= Bigstring.length bs then V_bool false
        else
          let raw = Bigstring.get bs byte_off |> Char.code in
          let v = (raw lsr bit_off) land 1 = 1 in
          let v = match fd.default_value with
            | Bool_default d -> v <> d
            | _ -> v
          in
          V_bool v
    | Int8 ->
        let off = data_start + fd.offset in
        if off >= Bigstring.length bs then V_int8 0
        else
          let raw = Char.code (Bigstring.get bs off) in
          let v = if raw > 127 then raw - 256 else raw in
          let v = match fd.default_value with
            | Int_default d -> v lxor (Int64.to_int d) | _ -> v in
          V_int8 v
    | Int16 ->
        let off = data_start + fd.offset in
        if off + 2 > Bigstring.length bs then V_int16 0
        else
          let raw = Bigstring.get_int16_le bs off in
          let v = match fd.default_value with
            | Int_default d -> raw lxor (Int64.to_int d) | _ -> raw in
          V_int16 v
    | Int32 ->
        let off = data_start + fd.offset in
        if off + 4 > Bigstring.length bs then V_int32 0l
        else
          let raw = Bigstring.get_int32_le bs off in
          let v = match fd.default_value with
            | Int_default d -> Int32.logxor raw (Int64.to_int32 d) | _ -> raw in
          V_int32 v
    | Int64 ->
        let off = data_start + fd.offset in
        if off + 8 > Bigstring.length bs then V_int64 0L
        else
          let raw = Bigstring.get_int64_le bs off in
          let v = match fd.default_value with
            | Int_default d -> Int64.logxor raw d | _ -> raw in
          V_int64 v
    | Uint8 ->
        let off = data_start + fd.offset in
        if off >= Bigstring.length bs then V_uint8 0
        else
          let raw = Char.code (Bigstring.get bs off) in
          let v = match fd.default_value with
            | Uint_default d -> raw lxor (Int64.to_int d) | _ -> raw in
          V_uint8 v
    | Uint16 ->
        let off = data_start + fd.offset in
        if off + 2 > Bigstring.length bs then V_uint16 0
        else
          let raw = Bigstring.get_int16_le bs off land 0xFFFF in
          let v = match fd.default_value with
            | Uint_default d -> raw lxor (Int64.to_int d land 0xFFFF)
            | _ -> raw in
          V_uint16 v
    | Uint32 ->
        let off = data_start + fd.offset in
        if off + 4 > Bigstring.length bs then V_uint32 0l
        else
          let raw = Bigstring.get_int32_le bs off in
          let v = match fd.default_value with
            | Uint_default d -> Int32.logxor raw (Int64.to_int32 d)
            | _ -> raw in
          V_uint32 v
    | Uint64 ->
        let off = data_start + fd.offset in
        if off + 8 > Bigstring.length bs then V_uint64 0L
        else
          let raw = Bigstring.get_int64_le bs off in
          let v = match fd.default_value with
            | Uint_default d -> Int64.logxor raw d | _ -> raw in
          V_uint64 v
    | Float32 ->
        let off = data_start + fd.offset in
        if off + 4 > Bigstring.length bs then V_float32 0.0
        else
          let raw_bits = Bigstring.get_int32_le bs off in
          let bits = match fd.default_value with
            | Float32_default d -> Int32.logxor raw_bits d
            | _ -> raw_bits
          in
          V_float32 (Int32.float_of_bits bits)
    | Float64 ->
        let off = data_start + fd.offset in
        if off + 8 > Bigstring.length bs then V_float64 0.0
        else
          let raw_bits = Bigstring.get_int64_le bs off in
          let bits = match fd.default_value with
            | Float64_default d -> Int64.logxor raw_bits d
            | _ -> raw_bits
          in
          V_float64 (Int64.float_of_bits bits)
    | Text | Data -> V_void

let read_field_from_blob (bs : bigstring)
    (fd : Schema_registry.field_descriptor) : (field_value, error) result =
  let* (data_start, _data_words, ptr_start, _ptr_count) =
    parse_struct_sections bs in
  Ok (read_field bs ~data_start ~ptr_start fd)

let read_field_by_name (bs : bigstring)
    (schema : Schema_registry.registered_schema) (field_name : string) :
    (field_value, error) result =
  match Hashtbl.find_opt schema.field_by_name field_name with
  | None -> Error (Storage_error ("unknown field: " ^ field_name))
  | Some fd -> read_field_from_blob bs fd
