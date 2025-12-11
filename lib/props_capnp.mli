(** Property storage using CapnProto serialization.
    
    Read path uses BigstringMessage with segments pointing directly into mmap. *)

open Types

type schema_kind = 
  | NodeSchemaKind
  | EdgeSchemaKind

module CapnpWireFormat : sig
  val to_bytes : 'a Capnp.BytesMessage.Message.t -> bigstring
  val from_bytes : bigstring -> (Capnp.Message.ro Bigstring_message.Message.t, error) result
end

val register_node_schema : t -> ?txn:[> `Read | `Write ] Lmdb.Txn.t -> string -> int64 -> (unit, error) result
val register_edge_schema : t -> ?txn:[> `Read | `Write ] Lmdb.Txn.t -> string -> int64 -> (unit, error) result
val get_schema_metadata : t -> ?txn:[> `Read ] Lmdb.Txn.t -> string -> (schema_kind * int64, error) result

val set_node_props_capnp : 
  t -> ?txn:[> `Read | `Write ] Lmdb.Txn.t -> node_id -> string -> 
  ('builder -> unit) -> (unit -> 'builder) -> 
  ('builder -> 'a Capnp.BytesMessage.Message.t) -> (unit, error) result

val get_node_props_capnp :
  t -> ?txn:[> `Read ] Lmdb.Txn.t -> node_id ->
  (Capnp.Message.ro Bigstring_message.Message.t -> 'reader) ->
  ('reader -> 'result) -> ('result, error) result

val set_edge_props_capnp :
  t -> ?txn:[> `Read | `Write ] Lmdb.Txn.t -> edge_id -> string ->
  ('builder -> unit) -> (unit -> 'builder) ->
  ('builder -> 'a Capnp.BytesMessage.Message.t) -> (unit, error) result

val get_edge_props_capnp :
  t -> ?txn:[> `Read ] Lmdb.Txn.t -> edge_id ->
  (Capnp.Message.ro Bigstring_message.Message.t -> 'reader) ->
  ('reader -> 'result) -> ('result, error) result

val get_edge_meta : t -> ?txn:[> `Read ] Lmdb.Txn.t -> edge_id -> ((intern_id * node_id * node_id), error) result
val get_node_meta : t -> ?txn:[> `Read ] Lmdb.Txn.t -> node_id -> (intern_id, error) result
