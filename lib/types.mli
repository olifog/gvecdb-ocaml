(** core type definitions for gvecdb *)

type bigstring = Common.bigstring
type id = int64
type intern_id = int64
type node_id = id
type edge_id = id
type vector_id = id
type vector_tag_id = intern_id

type owner_kind = Node | Edge

(** packed owner ID: bit 63 = type (0=node, 1=edge), bits 0-62 = ID *)
module Owner : sig
  val encode : owner_kind -> id -> int64
  val decode : int64 -> owner_kind * id
  val kind_to_string : owner_kind -> string
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

module Error : sig
  val to_string : error -> string
  val pp : Format.formatter -> error -> unit
end

val ( let* ) : ('a, 'b) result -> ('a -> ('c, 'b) result) -> ('c, 'b) result
val ( let+ ) : ('a, 'b) result -> ('a -> 'c) -> ('c, 'b) result
val wrap_lmdb_exn : (unit -> 'a) -> ('a, error) result

type 'perm txn = 'perm Lmdb.Txn.t constraint 'perm = [< `Read | `Write ]
type ro_txn = [ `Read ] txn
type rw_txn = [ `Read | `Write ] txn

(** database handle. fields exposed for internal library modules only *)
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
  vector_file : Vector_file.t;
  vector_index : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t;
  vector_owners : (bigstring, bigstring, [ `Uni ]) Lmdb.Map.t;
}

val with_transaction : t -> (rw_txn -> 'a) -> 'a option
val with_transaction_ro : t -> (ro_txn -> 'a) -> 'a option
val abort_transaction : 'perm txn -> 'a

module Metadata : sig
  val version : string
  val next_node_id : string
  val next_edge_id : string
  val next_intern_id : string
  val next_vector_id : string
end

val db_version : int64
