(** main API for gvecdb *)

(** {1 core types} *)

type bigstring = Common.bigstring
type id = int64
type intern_id = int64
type node_id = id
type edge_id = id
type vector_id = id
type vector_tag_id = intern_id
type owner_kind = Node | Edge
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

type distance_metric =
  | Euclidean
  | Cosine
  | DotProduct  (** negative dot product *)

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

(** {1 schema registration} *)

module Schema_registry = Schema_registry
module Dynamic_reader = Dynamic_reader
module Filter = Filter

val register_schema_from_capnp :
  t ->
  kind:Schema_registry.schema_kind ->
  type_name:string ->
  capnp_path:string ->
  struct_name:string ->
  ?txn:[> `Read | `Write ] txn ->
  unit ->
  (Schema_registry.registered_schema, error) result
(** register a node/edge schema by compiling a .capnp file. extracts field
    offsets from the capnp compiler's CodeGeneratorRequest output. *)

val register_schema_from_fields :
  t ->
  kind:Schema_registry.schema_kind ->
  type_name:string ->
  data_word_count:int ->
  pointer_count:int ->
  fields:Schema_registry.field_descriptor list ->
  ?txn:[> `Read | `Write ] txn ->
  unit ->
  (Schema_registry.registered_schema, error) result
(** register a schema with explicit field descriptors (no .capnp file needed) *)

val get_schema :
  t ->
  ?txn:[> `Read ] txn ->
  string ->
  (Schema_registry.registered_schema, error) result

val load_all_schemas : t -> unit
(** load all persisted schemas into the in-memory cache *)

(** {1 adjacency queries} *)

val get_outbound_edges :
  t ->
  ?txn:[> `Read ] txn ->
  node_id ->
  ?edge_type:string ->
  ?filters:Filter.filter_predicate list ->
  unit ->
  (edge_info list, error) result

val get_inbound_edges :
  t ->
  ?txn:[> `Read ] txn ->
  node_id ->
  ?edge_type:string ->
  ?filters:Filter.filter_predicate list ->
  unit ->
  (edge_info list, error) result

(** {1 node properties} *)

val set_node_props :
  t ->
  ?txn:[> `Read | `Write ] txn ->
  node_id ->
  string ->
  bigstring ->
  (unit, error) result
(** set raw property bytes on an existing node. [string] is the type_name used
    to update node_meta. *)

val get_node_props :
  t -> ?txn:[> `Read ] txn -> node_id -> (bigstring, error) result

(** {1 edge properties} *)

val set_edge_props :
  t ->
  ?txn:[> `Read | `Write ] txn ->
  edge_id ->
  bigstring ->
  (unit, error) result
(** set raw property bytes on an existing edge. does not change edge type. *)

val get_edge_props :
  t -> ?txn:[> `Read ] txn -> edge_id -> (bigstring, error) result

(** {1 vectors} *)

val create_vector :
  t ->
  txn:[> `Read | `Write ] txn ->
  ?normalize:bool ->
  ?metric:distance_metric ->
  ?hnsw_params:Hnsw.params ->
  owner_kind ->
  id ->
  string ->
  bigstring ->
  (vector_id, error) result
(** create vector on a node or edge. [~normalize:true] (default) stores
    unit-length vectors for fast cosine similarity with original magnitude
    preserved in metadata. [~metric] and [~hnsw_params] apply only when the HNSW
    index is first created for this vector tag. requires explicit transaction *)

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

val get_vectors :
  t ->
  ?txn:[> `Read ] txn ->
  owner_kind ->
  id ->
  ?vector_tag:string ->
  unit ->
  (vector_info list, error) result

(** {1 k-NN search} *)

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

(** {1 HNSW-based k-NN search} *)

val knn_hnsw :
  t ->
  ?txn:[> `Read ] txn ->
  metric:distance_metric ->
  k:int ->
  ef:int ->
  vector_tag:string ->
  float array ->
  (knn_result list, error) result
(** HNSW approximate k-NN. ef controls search quality (higher = better recall).
    Searches only vectors with the specified tag *)

val rebuild_hnsw_index :
  t ->
  ?txn:rw_txn ->
  ?hnsw_params:Hnsw.params ->
  vector_tag:string ->
  unit ->
  (unit, error) result
(** rebuild HNSW index for a tag from scratch using all vectors with that tag.
    requires a write transaction because it updates the hnsw_slots mapping. *)

(** {1 dynamic property access} *)

val read_node_field :
  t ->
  ?txn:[> `Read ] txn ->
  node_id ->
  string ->
  (Dynamic_reader.field_value, error) result
(** read a single field from a node's properties by name *)

val read_edge_field :
  t ->
  ?txn:[> `Read ] txn ->
  edge_id ->
  string ->
  (Dynamic_reader.field_value, error) result

(** {1 Internal modules (exposed for testing)} *)

module Types = Types
module Hnsw_page = Hnsw_page
module Hnsw_mvcc = Hnsw_mvcc
module Hnsw = Hnsw
