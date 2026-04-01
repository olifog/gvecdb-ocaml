open Types
module Bigstring = Bigstringaf
module Schema = Schema.Make (Capnp.BytesMessage)

type schema_kind = Props_capnp.schema_kind = NodeSchemaKind | EdgeSchemaKind

type field_type =
  | Void
  | Bool
  | Int8
  | Int16
  | Int32
  | Int64
  | Uint8
  | Uint16
  | Uint32
  | Uint64
  | Float32
  | Float64
  | Text
  | Data

type default_value =
  | No_default
  | Bool_default of bool
  | Int_default of int64
  | Uint_default of int64
  | Float32_default of int32
  | Float64_default of int64
  | Text_default of string

type field_descriptor = {
  name : string;
  field_type : field_type;
  offset : int;
  is_pointer : bool;
  default_value : default_value;
}

type registered_schema = {
  type_name : string;
  kind : Props_capnp.schema_kind;
  data_word_count : int;
  pointer_count : int;
  fields : field_descriptor list;
  field_by_name : (string, field_descriptor) Hashtbl.t;
}

let field_type_to_int = function
  | Void -> 0 | Bool -> 1 | Int8 -> 2 | Int16 -> 3 | Int32 -> 4
  | Int64 -> 5 | Uint8 -> 6 | Uint16 -> 7 | Uint32 -> 8
  | Uint64 -> 9 | Float32 -> 10 | Float64 -> 11 | Text -> 12 | Data -> 13

let field_type_of_int_opt = function
  | 0 -> Some Void | 1 -> Some Bool | 2 -> Some Int8 | 3 -> Some Int16
  | 4 -> Some Int32 | 5 -> Some Int64 | 6 -> Some Uint8 | 7 -> Some Uint16
  | 8 -> Some Uint32 | 9 -> Some Uint64 | 10 -> Some Float32
  | 11 -> Some Float64 | 12 -> Some Text | 13 -> Some Data
  | _ -> None

let field_type_of_int n =
  match field_type_of_int_opt n with Some t -> t | None -> Void

let field_type_is_pointer = function
  | Text | Data -> true
  | _ -> false

let extract_field_type (typ : Schema.Reader.Type.t) : field_type option =
  match Schema.Reader.Type.get typ with
  | Schema.Reader.Type.Void -> Some Void
  | Schema.Reader.Type.Bool -> Some Bool
  | Schema.Reader.Type.Int8 -> Some Int8
  | Schema.Reader.Type.Int16 -> Some Int16
  | Schema.Reader.Type.Int32 -> Some Int32
  | Schema.Reader.Type.Int64 -> Some Int64
  | Schema.Reader.Type.Uint8 -> Some Uint8
  | Schema.Reader.Type.Uint16 -> Some Uint16
  | Schema.Reader.Type.Uint32 -> Some Uint32
  | Schema.Reader.Type.Uint64 -> Some Uint64
  | Schema.Reader.Type.Float32 -> Some Float32
  | Schema.Reader.Type.Float64 -> Some Float64
  | Schema.Reader.Type.Text -> Some Text
  | Schema.Reader.Type.Data -> Some Data
  | _ -> None

(* store defaults as raw bits so XOR is always correct regardless of
   whether the default is zero, negative zero and allat *)
let extract_default_value (_ft : field_type) (value : Schema.Reader.Value.t) :
    default_value =
  match Schema.Reader.Value.get value with
  | Schema.Reader.Value.Bool b when b -> Bool_default true
  | Schema.Reader.Value.Int8 v when v <> 0 -> Int_default (Int64.of_int v)
  | Schema.Reader.Value.Int16 v when v <> 0 -> Int_default (Int64.of_int v)
  | Schema.Reader.Value.Int32 v when v <> 0l -> Int_default (Int64.of_int32 v)
  | Schema.Reader.Value.Int64 v when v <> 0L -> Int_default v
  | Schema.Reader.Value.Uint8 v when v <> 0 -> Uint_default (Int64.of_int v)
  | Schema.Reader.Value.Uint16 v when v <> 0 -> Uint_default (Int64.of_int v)
  | Schema.Reader.Value.Uint32 v ->
      let i = Stdint.Uint32.to_int64 v in
      if i <> 0L then Uint_default i else No_default
  | Schema.Reader.Value.Uint64 v ->
      let i = Stdint.Uint64.to_int64 v in
      if i <> 0L then Uint_default i else No_default
  | Schema.Reader.Value.Float32 v ->
      let bits = Int32.bits_of_float v in
      if bits <> 0l then Float32_default bits else No_default
  | Schema.Reader.Value.Float64 v ->
      let bits = Int64.bits_of_float v in
      if bits <> 0L then Float64_default bits else No_default
  | Schema.Reader.Value.Text s ->
      if String.length s > 0 then Text_default s else No_default
  | _ -> No_default

(* for Bool fields, store the raw capnp bit offset so we can split
   byte/bit at read time for all other data fields convert to bytes. *)
let offset_for_storage ft raw_offset =
  if field_type_is_pointer ft then raw_offset * 8
  else
    match ft with
    | Bool -> raw_offset
    | Int8 | Uint8 -> raw_offset
    | Int16 | Uint16 -> raw_offset * 2
    | Int32 | Uint32 | Float32 -> raw_offset * 4
    | Int64 | Uint64 | Float64 -> raw_offset * 8
    | _ -> 0

let parse_capnp_file (capnp_path : string) (struct_name : string) :
    (int * int * field_descriptor list, error) result =
  let cmd = Printf.sprintf "capnp compile -o- %s" (Filename.quote capnp_path) in
  let ic = Unix.open_process_in cmd in
  let buf = Buffer.create 4096 in
  (try
     let chunk = Bytes.create 4096 in
     let rec loop () =
       let n = input ic chunk 0 4096 in
       if n > 0 then begin
         Buffer.add_subbytes buf chunk 0 n;
         loop ()
       end
     in
     loop ()
   with End_of_file -> ());
  let status = Unix.close_process_in ic in
  match status with
  | Unix.WEXITED 0 ->
      let bytes = Buffer.contents buf in
      let stream = Capnp.Codecs.FramedStream.empty `None in
      Capnp.Codecs.FramedStream.add_fragment stream bytes;
      (match Capnp.Codecs.FramedStream.get_next_frame stream with
       | Result.Ok msg ->
           let req = Schema.Reader.CodeGeneratorRequest.of_message msg in
           let nodes = Schema.Reader.CodeGeneratorRequest.nodes_get_list req in
           let suffix = ":" ^ struct_name in
           let slen = String.length suffix in
           let target =
             List.find_opt (fun node ->
               let dn = Schema.Reader.Node.display_name_get node in
               String.length dn >= slen &&
               String.sub dn (String.length dn - slen) slen = suffix)
               nodes
           in
           (match target with
            | None -> Error (Storage_error ("struct not found: " ^ struct_name))
            | Some node ->
                match Schema.Reader.Node.get node with
                | Schema.Reader.Node.Struct s ->
                    let dwc = Schema.Reader.Node.Struct.data_word_count_get s in
                    let pc = Schema.Reader.Node.Struct.pointer_count_get s in
                    let fields_raw = Schema.Reader.Node.Struct.fields_get_list s in
                    let fields = List.filter_map (fun field ->
                      match Schema.Reader.Field.get field with
                      | Schema.Reader.Field.Slot slot ->
                          let name = Schema.Reader.Field.name_get field in
                          let offset_raw = Stdint.Uint32.to_int
                              (Schema.Reader.Field.Slot.offset_get slot) in
                          let typ = Schema.Reader.Field.Slot.type_get slot in
                          (match extract_field_type typ with
                           | None -> None
                           | Some ft ->
                               let is_pointer = field_type_is_pointer ft in
                               let offset = offset_for_storage ft offset_raw in
                               let dv = extract_default_value ft
                                   (Schema.Reader.Field.Slot.default_value_get slot) in
                               Some { name; field_type = ft; offset;
                                      is_pointer; default_value = dv })
                      | _ -> None) fields_raw
                    in
                    Ok (dwc, pc, fields)
                | _ -> Error (Storage_error ("not a struct: " ^ struct_name)))
       | Result.Error _ ->
           Error (Storage_error "failed to parse CodeGeneratorRequest"))
  | _ -> Error (Storage_error ("capnp compile failed for: " ^ capnp_path))

let serialize_field_descriptor (fd : field_descriptor) : string =
  let buf = Buffer.create (32 + String.length fd.name) in
  Buffer.add_uint16_le buf (String.length fd.name);
  Buffer.add_string buf fd.name;
  Buffer.add_uint8 buf (field_type_to_int fd.field_type);
  Buffer.add_int32_le buf (Int32.of_int fd.offset);
  Buffer.add_uint8 buf (if fd.is_pointer then 1 else 0);
  (match fd.default_value with
   | No_default -> Buffer.add_uint8 buf 0
   | Bool_default b ->
       Buffer.add_uint8 buf 1;
       Buffer.add_uint8 buf (if b then 1 else 0)
   | Int_default v ->
       Buffer.add_uint8 buf 2;
       Buffer.add_int64_le buf v
   | Uint_default v ->
       Buffer.add_uint8 buf 3;
       Buffer.add_int64_le buf v
   | Float32_default bits ->
       Buffer.add_uint8 buf 4;
       Buffer.add_int32_le buf bits
   | Float64_default bits ->
       Buffer.add_uint8 buf 5;
       Buffer.add_int64_le buf bits
   | Text_default s ->
       Buffer.add_uint8 buf 6;
       Buffer.add_uint16_le buf (String.length s);
       Buffer.add_string buf s);
  Buffer.contents buf

let deserialize_field_descriptor (s : string) (pos : int ref) :
    (field_descriptor, error) result =
  let len = String.length s in
  let need n = if !pos + n > len then
    Error (Corrupted_data "truncated schema field descriptor")
  else Ok () in
  let* () = need 2 in
  let name_len = Char.code s.[!pos] lor (Char.code s.[!pos + 1] lsl 8) in
  pos := !pos + 2;
  let* () = need name_len in
  let name = String.sub s !pos name_len in
  pos := !pos + name_len;
  let* () = need 6 in
  let field_type = field_type_of_int (Char.code s.[!pos]) in
  pos := !pos + 1;
  let offset = Int32.to_int (String.get_int32_le s !pos) in
  pos := !pos + 4;
  let is_pointer = Char.code s.[!pos] = 1 in
  pos := !pos + 1;
  let* () = need 1 in
  let dv_tag = Char.code s.[!pos] in
  pos := !pos + 1;
  let* default_value = match dv_tag with
    | 0 -> Ok No_default
    | 1 ->
        let* () = need 1 in
        let b = Char.code s.[!pos] = 1 in
        pos := !pos + 1; Ok (Bool_default b)
    | 2 ->
        let* () = need 8 in
        let v = String.get_int64_le s !pos in
        pos := !pos + 8; Ok (Int_default v)
    | 3 ->
        let* () = need 8 in
        let v = String.get_int64_le s !pos in
        pos := !pos + 8; Ok (Uint_default v)
    | 4 ->
        let* () = need 4 in
        let bits = String.get_int32_le s !pos in
        pos := !pos + 4; Ok (Float32_default bits)
    | 5 ->
        let* () = need 8 in
        let bits = String.get_int64_le s !pos in
        pos := !pos + 8; Ok (Float64_default bits)
    | 6 ->
        let* () = need 2 in
        let slen = Char.code s.[!pos] lor (Char.code s.[!pos + 1] lsl 8) in
        pos := !pos + 2;
        let* () = need slen in
        let sv = String.sub s !pos slen in
        pos := !pos + slen; Ok (Text_default sv)
    | _ -> Ok No_default
  in
  Ok { name; field_type; offset; is_pointer; default_value }

let serialize_schema (dwc : int) (pc : int) (fields : field_descriptor list) :
    string =
  let buf = Buffer.create 256 in
  Buffer.add_uint16_le buf dwc;
  Buffer.add_uint16_le buf pc;
  Buffer.add_uint16_le buf (List.length fields);
  List.iter (fun fd -> Buffer.add_string buf (serialize_field_descriptor fd))
    fields;
  Buffer.contents buf

let deserialize_schema (s : string) :
    (int * int * field_descriptor list, error) result =
  if String.length s < 6 then
    Error (Corrupted_data "truncated schema header")
  else
    let dwc = Char.code s.[0] lor (Char.code s.[1] lsl 8) in
    let pc = Char.code s.[2] lor (Char.code s.[3] lsl 8) in
    let n = Char.code s.[4] lor (Char.code s.[5] lsl 8) in
    let pos = ref 6 in
    let rec read_fields acc remaining =
      if remaining = 0 then Ok (List.rev acc)
      else
        let* fd = deserialize_field_descriptor s pos in
        read_fields (fd :: acc) (remaining - 1)
    in
    let* fields = read_fields [] n in
    Ok (dwc, pc, fields)

let make_registered_schema type_name kind dwc pc fields =
  let field_by_name = Hashtbl.create 16 in
  List.iter (fun fd -> Hashtbl.replace field_by_name fd.name fd) fields;
  { type_name; kind; data_word_count = dwc; pointer_count = pc;
    fields; field_by_name }

(* In-memory cache -- keyed by type_name *)
let schema_cache : (string, registered_schema) Hashtbl.t = Hashtbl.create 16

let register_schema_from_capnp (db : t)
    ~(kind : Props_capnp.schema_kind) ~(type_name : string)
    ~(capnp_path : string) ~(struct_name : string) ?txn () :
    (registered_schema, error) result =
  let* (dwc, pc, fields) = parse_capnp_file capnp_path struct_name in
  let serialized = serialize_schema dwc pc fields in
  let schema_key = "schema_fields:" ^ type_name in
  let* () = wrap_lmdb_exn (fun () ->
      let bs = Bigstring.of_string ~off:0 ~len:(String.length serialized)
          serialized in
      Lmdb.Map.set db.metadata ?txn schema_key bs) in
  let* () = match kind with
    | Props_capnp.NodeSchemaKind ->
        Props_capnp.register_node_schema db ?txn type_name 0L
    | Props_capnp.EdgeSchemaKind ->
        Props_capnp.register_edge_schema db ?txn type_name 0L
  in
  let schema = make_registered_schema type_name kind dwc pc fields in
  Hashtbl.replace schema_cache type_name schema;
  Ok schema

let register_schema_from_fields (db : t)
    ~(kind : Props_capnp.schema_kind) ~(type_name : string)
    ~(data_word_count : int) ~(pointer_count : int)
    ~(fields : field_descriptor list) ?txn () :
    (registered_schema, error) result =
  let serialized = serialize_schema data_word_count pointer_count fields in
  let schema_key = "schema_fields:" ^ type_name in
  let* () = wrap_lmdb_exn (fun () ->
      let bs = Bigstring.of_string ~off:0 ~len:(String.length serialized)
          serialized in
      Lmdb.Map.set db.metadata ?txn schema_key bs) in
  let* () = match kind with
    | Props_capnp.NodeSchemaKind ->
        Props_capnp.register_node_schema db ?txn type_name 0L
    | Props_capnp.EdgeSchemaKind ->
        Props_capnp.register_edge_schema db ?txn type_name 0L
  in
  let schema = make_registered_schema type_name kind data_word_count
      pointer_count fields in
  Hashtbl.replace schema_cache type_name schema;
  Ok schema

let get_schema (db : t) ?txn (type_name : string) :
    (registered_schema, error) result =
  match Hashtbl.find_opt schema_cache type_name with
  | Some schema -> Ok schema
  | None ->
      let schema_key = "schema_fields:" ^ type_name in
      (try
         let bs = Lmdb.Map.get db.metadata ?txn schema_key in
         let s = Bigstring.to_string bs in
         let* (dwc, pc, fields) = deserialize_schema s in
         let* kind_result = Props_capnp.get_schema_metadata db ?txn type_name in
         let kind = fst kind_result in
         let schema = make_registered_schema type_name kind dwc pc fields in
         Hashtbl.replace schema_cache type_name schema;
         Ok schema
       with
       | Not_found | Lmdb.Not_found ->
           Error (Storage_error ("schema not registered: " ^ type_name))
       | Invalid_argument msg -> Error (Corrupted_data msg))

let load_all_schemas (db : t) : unit =
  ignore (Types.with_transaction_ro db (fun txn ->
    let prefix = "schema_fields:" in
    let plen = String.length prefix in
    try
      Lmdb.Cursor.go Lmdb.Ro ~txn db.metadata (fun cursor ->
        let process_entry key =
          if String.length key >= plen &&
             String.sub key 0 plen = prefix then
            let type_name = String.sub key plen (String.length key - plen) in
            ignore (get_schema db ~txn type_name)
        in
        let rec scan () =
          match Lmdb.Cursor.next cursor with
          | key, _ -> process_entry key; scan ()
          | exception Lmdb.Not_found -> ()
        in
        (match Lmdb.Cursor.first cursor with
         | key, _ -> process_entry key; scan ()
         | exception Lmdb.Not_found -> ()))
    with Not_found | Lmdb.Not_found -> ()))
