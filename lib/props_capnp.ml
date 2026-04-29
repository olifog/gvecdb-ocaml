open Types
module Bigstring = Bigstringaf

type schema_kind = NodeSchemaKind | EdgeSchemaKind

let register_node_schema (db : t) ?txn (type_name : string) (schema_id : int64)
    : (unit, error) result =
  wrap_lmdb_exn (fun () ->
      let key = "schema:" ^ type_name in
      let buf = Bigstring.create 9 in
      Bigstring.set buf 0 '\x00';
      Bigstring.set_int64_be buf 1 schema_id;
      Lmdb.Map.set db.metadata ?txn key buf)

let register_edge_schema (db : t) ?txn (type_name : string) (schema_id : int64)
    : (unit, error) result =
  wrap_lmdb_exn (fun () ->
      let key = "schema:" ^ type_name in
      let buf = Bigstring.create 9 in
      Bigstring.set buf 0 '\x01';
      Bigstring.set_int64_be buf 1 schema_id;
      Lmdb.Map.set db.metadata ?txn key buf)

let get_schema_metadata (db : t) ?txn (type_name : string) :
    (schema_kind * int64, error) result =
  let key = "schema:" ^ type_name in
  try
    let value = Lmdb.Map.get db.metadata ?txn key in
    if Bigstring.length value < 9 then
      Error (Corrupted_data "schema metadata too short")
    else
      let kind =
        match Bigstring.get value 0 with
        | '\x00' -> NodeSchemaKind
        | '\x01' -> EdgeSchemaKind
        | _ -> raise Exit
      in
      let schema_id = Bigstring.get_int64_be value 1 in
      Ok (kind, schema_id)
  with
  | Exit -> Error (Corrupted_data "invalid schema kind byte")
  | Not_found | Lmdb.Not_found ->
      Error (Storage_error ("schema not found: " ^ type_name))

let get_edge_meta (db : t) ?txn (edge_id : edge_id) :
    (intern_id * node_id * node_id, error) result =
  let key = Keys.encode_id_bs edge_id in
  try
    let meta = Lmdb.Map.get db.edge_meta ?txn key in
    Ok (Keys.decode_edge_meta meta)
  with
  | Not_found | Lmdb.Not_found -> Error (Edge_not_found edge_id)
  | Invalid_argument msg -> Error (Corrupted_data msg)

let get_node_meta (db : t) ?txn (node_id : node_id) : (intern_id, error) result
    =
  let key = Keys.encode_id_bs node_id in
  try
    let meta = Lmdb.Map.get db.node_meta ?txn key in
    Ok (Keys.decode_id_bs meta)
  with
  | Not_found | Lmdb.Not_found -> Error (Node_not_found node_id)
  | Invalid_argument msg -> Error (Corrupted_data msg)
