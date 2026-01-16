(** main API for gvecdb *)

(** {1 core types} *)

module Bigstring_message : Capnp.MessageSig.S

type bigstring = Common.bigstring
type id = int64
type intern_id = int64
type node_id = id
type edge_id = id
type vector_id = id
type vector_tag_id = intern_id
type owner_kind = Node | Edge

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

type t

(** {1 errors} *)

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

(** {1 transactions} *)

type 'perm txn = 'perm Lmdb.Txn.t constraint 'perm = [< `Read | `Write ]
type ro_txn = [ `Read ] txn
type rw_txn = [ `Read | `Write ] txn

val with_transaction : t -> (rw_txn -> 'a) -> 'a option
(** run [f] in a read-write transaction. returns [None] if aborted *)

val with_transaction_ro : t -> (ro_txn -> 'a) -> 'a option
val abort_transaction : 'perm txn -> 'a

(** {1 database lifecycle} *)

val create : ?map_size:int -> string -> (t, error) result
(** create or open database. [map_size] is max size in bytes (default 10GB) *)

val close : t -> unit

(** {1 nodes} *)

val create_node :
  t -> ?txn:[> `Read | `Write ] txn -> string -> (node_id, error) result

val node_exists : t -> ?txn:[> `Read ] txn -> node_id -> (bool, error) result

val get_node_info :
  t -> ?txn:[> `Read ] txn -> node_id -> (node_info, error) result

val delete_node :
  t -> ?txn:[> `Read | `Write ] txn -> node_id -> (unit, error) result
(** cascade deletes all attached vectors, connected edges, and their vectors *)

(** {1 edges} *)

val create_edge :
  t ->
  ?txn:[> `Read | `Write ] txn ->
  string ->
  node_id ->
  node_id ->
  (edge_id, error) result

val edge_exists : t -> ?txn:[> `Read ] txn -> edge_id -> (bool, error) result

val delete_edge :
  t -> ?txn:[> `Read | `Write ] txn -> edge_id -> (unit, error) result
(** cascade deletes all attached vectors *)

val get_edge_info :
  t -> ?txn:[> `Read ] txn -> edge_id -> (edge_info, error) result

(** {1 adjacency queries} *)

val get_outbound_edges :
  t -> ?txn:[> `Read ] txn -> node_id -> (edge_info list, error) result

val get_inbound_edges :
  t -> ?txn:[> `Read ] txn -> node_id -> (edge_info list, error) result

val get_outbound_edges_by_type :
  t ->
  ?txn:[> `Read ] txn ->
  node_id ->
  string ->
  (edge_info list, error) result

val get_inbound_edges_by_type :
  t ->
  ?txn:[> `Read ] txn ->
  node_id ->
  string ->
  (edge_info list, error) result

(** {1 capnproto schemas} *)

val register_node_schema_capnp :
  t -> ?txn:[> `Read | `Write ] txn -> string -> int64 -> (unit, error) result

val register_edge_schema_capnp :
  t -> ?txn:[> `Read | `Write ] txn -> string -> int64 -> (unit, error) result

(** {1 node properties} *)

val set_node_props_capnp :
  t ->
  ?txn:[> `Read | `Write ] txn ->
  node_id ->
  string ->
  ('builder -> unit) ->
  (unit -> 'builder) ->
  ('builder -> 'a Capnp.BytesMessage.Message.t) ->
  (unit, error) result

val get_node_props_capnp :
  t ->
  ?txn:[> `Read ] txn ->
  node_id ->
  (Capnp.Message.ro Bigstring_message.Message.t -> 'reader) ->
  ('reader -> 'result) ->
  ('result, error) result

(** {1 edge properties} *)

val set_edge_props_capnp :
  t ->
  ?txn:[> `Read | `Write ] txn ->
  edge_id ->
  string ->
  ('builder -> unit) ->
  (unit -> 'builder) ->
  ('builder -> 'a Capnp.BytesMessage.Message.t) ->
  (unit, error) result

val get_edge_props_capnp :
  t ->
  ?txn:[> `Read ] txn ->
  edge_id ->
  (Capnp.Message.ro Bigstring_message.Message.t -> 'reader) ->
  ('reader -> 'result) ->
  ('result, error) result

(** {1 vectors} *)

val create_vector :
  t ->
  txn:[> `Read | `Write ] txn ->
  ?normalize:bool ->
  node_id ->
  string ->
  bigstring ->
  (vector_id, error) result
(** create vector on a node. [~normalize:true] (default) stores unit-length
    vectors for fast cosine similarity with original magnitude preserved in
    metadata. requires explicit transaction *)

val create_edge_vector :
  t ->
  txn:[> `Read | `Write ] txn ->
  ?normalize:bool ->
  edge_id ->
  string ->
  bigstring ->
  (vector_id, error) result

val vector_exists :
  t -> ?txn:[> `Read ] txn -> vector_id -> (bool, error) result

val get_vector :
  t -> ?txn:[> `Read ] txn -> vector_id -> (bigstring, error) result
(** returns normalized vector if stored with [~normalize:true]. zero-copy view
    into mmap, only valid within current transaction *)

val get_vector_info :
  t -> ?txn:[> `Read ] txn -> vector_id -> (vector_info, error) result

val delete_vector :
  t -> txn:[> `Read | `Write ] txn -> vector_id -> (unit, error) result

val get_vectors_for_node :
  t ->
  ?txn:[> `Read ] txn ->
  node_id ->
  ?vector_tag:string ->
  unit ->
  (vector_info list, error) result

val get_vectors_for_edge :
  t ->
  ?txn:[> `Read ] txn ->
  edge_id ->
  ?vector_tag:string ->
  unit ->
  (vector_info list, error) result

(** {1 k-NN search} *)

type distance_metric =
  | Euclidean
  | Cosine
  | DotProduct  (** negative dot product *)

type knn_result = {
  vector_id : vector_id;
  owner_kind : owner_kind;
  owner_id : id;
  vector_tag : string;
  distance : float;
}

val knn_brute_force :
  t ->
  ?txn:[> `Read ] txn ->
  metric:distance_metric ->
  k:int ->
  float array ->
  (knn_result list, error) result
(** brute-force k-NN. O(n log k). results sorted by distance ascending *)

val knn_brute_force_bs :
  t ->
  ?txn:[> `Read ] txn ->
  metric:distance_metric ->
  k:int ->
  bigstring ->
  (knn_result list, error) result
