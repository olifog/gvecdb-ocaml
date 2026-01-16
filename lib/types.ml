type bigstring = Common.bigstring
type id = int64
type intern_id = int64
type node_id = id
type edge_id = id
type vector_id = id
type vector_tag_id = intern_id
type owner_kind = Node | Edge

(* Packed owner ID encoding: bit 63 indicates type (0=node, 1=edge), bits 0-62
   are the ID *)
module Owner = struct
  let edge_flag = 0x8000_0000_0000_0000L (* bit 63 set *)
  let id_mask = 0x7FFF_FFFF_FFFF_FFFFL (* bits 0-62 *)

  let encode kind id =
    if Int64.logand id edge_flag <> 0L then
      invalid_arg "Owner.encode: id exceeds 63 bits";
    match kind with Node -> id | Edge -> Int64.logor edge_flag id

  let decode packed =
    let kind = if Int64.logand packed edge_flag <> 0L then Edge else Node in
    let id = Int64.logand packed id_mask in
    (kind, id)

  let kind_to_string = function Node -> "node" | Edge -> "edge"
end

type node_info = { id : node_id; node_type : string }

type edge_info = {
  id : edge_id;
  edge_type : string;
  src : node_id;
  dst : node_id;
}

type vector_info = {
  vector_id : vector_id;
  owner_kind : owner_kind;
  owner_id : id;
  vector_tag : string;
}

type distance_metric = Euclidean | Cosine | DotProduct

type knn_result = {
  vector_id : vector_id;
  owner_kind : owner_kind;
  owner_id : id;
  vector_tag : string;
  distance : float;
}

type error =
  | Node_not_found of node_id
  | Edge_not_found of edge_id
  | Vector_not_found of vector_id
  | Storage_full
  | Storage_error of string
  | Corrupted_data of string

module Error = struct
  let to_string = function
    | Node_not_found id -> Printf.sprintf "node not found: %Ld" id
    | Edge_not_found id -> Printf.sprintf "edge not found: %Ld" id
    | Vector_not_found id -> Printf.sprintf "vector not found: %Ld" id
    | Storage_full -> "storage full"
    | Storage_error msg -> Printf.sprintf "storage error: %s" msg
    | Corrupted_data msg -> Printf.sprintf "corrupted data: %s" msg

  let pp fmt e = Format.pp_print_string fmt (to_string e)
end

let ( let* ) = Result.bind
let ( let+ ) r f = Result.map f r

let wrap_lmdb_exn f =
  try Ok (f ()) with
  | Not_found | Lmdb.Not_found ->
      Error (Corrupted_data "unexpected not found in storage operation")
  | Lmdb.Map_full -> Error Storage_full
  | Lmdb.Error code ->
      Error (Storage_error (Format.asprintf "%a" Lmdb.pp_error code))
  | Invalid_argument msg -> Error (Storage_error msg)

type 'perm txn = 'perm Lmdb.Txn.t constraint 'perm = [< `Read | `Write ]
type ro_txn = [ `Read ] txn
type rw_txn = [ `Read | `Write ] txn

type t = {
  env : Lmdb.Env.t;
  nodes : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t;
  edges : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t;
  node_meta : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t;
  edge_meta : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t;
  outbound : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t;
  inbound : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t;
  intern_forward : (string, bigstring, [ `Uni ]) Lmdb.Map.t;
  intern_reverse : (bigstring, string, [ `Uni ]) Lmdb.Map.t;
  metadata : (string, bigstring, [ `Uni ]) Lmdb.Map.t;
  vector_file : Vector_file.t; (* mmap'd vector storage *)
  vector_index : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t;
  vector_owners : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t;
}
(* nodes/edges store raw CapnProto message bytes
   node_meta/edge_meta store compact binary metadata
   adjacency indexes enable efficient graph traversal
   vector_file stores actual vector data in a separate mmap'd file *)

module Metadata = struct
  let version = "version"
  let next_node_id = "next_node_id"
  let next_edge_id = "next_edge_id"
  let next_intern_id = "next_intern_id"
  let next_vector_id = "next_vector_id"
end

let db_version = 2L

let with_transaction (db : t) (f : rw_txn -> 'a) : 'a option =
  Lmdb.Txn.go Lmdb.Rw db.env f

let with_transaction_ro (db : t) (f : ro_txn -> 'a) : 'a option =
  Lmdb.Txn.go Lmdb.Ro db.env f

let abort_transaction : 'perm txn -> 'a = Lmdb.Txn.abort
