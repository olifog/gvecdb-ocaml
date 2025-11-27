(** property operations using capnproto builder/reader api *)

open Types

(** schema kind for lmdb storage *)
type schema_kind = 
  | NodeSchemaKind
  | EdgeSchemaKind

(** capnproto serialization utilities *)
module CapnpCodec : sig
  val serialize : 'a Capnp.BytesMessage.Message.t -> string
  val deserialize : string -> Capnp.Message.ro Capnp.BytesMessage.Message.t
end

(** register a node schema *)
val register_node_schema : t -> ?txn:[> `Read | `Write ] Lmdb.Txn.t -> string -> int64 -> unit

(** register an edge schema *)
val register_edge_schema : t -> ?txn:[> `Read | `Write ] Lmdb.Txn.t -> string -> int64 -> unit

(** get schema metadata *)
val get_schema_metadata : t -> ?txn:[> `Read ] Lmdb.Txn.t -> string -> (schema_kind * int64) option

(** set node properties using capnproto builder api *)
val set_node_props_capnp : 
  t -> ?txn:[> `Read | `Write ] Lmdb.Txn.t -> node_id -> string -> 
  ('builder -> unit) -> 
  (unit -> 'builder) -> 
  ('builder -> 'a Capnp.BytesMessage.Message.t) -> 
  unit

(** get node properties using capnproto reader api with zero-copy from lmdb *)
val get_node_props_capnp :
  t -> ?txn:[> `Read ] Lmdb.Txn.t -> node_id -> string ->
  (Capnp.Message.ro Capnp.BytesMessage.Message.t -> 'reader) ->
  ('reader -> 'result) ->
  'result

(** set edge properties using capnproto builder api *)
val set_edge_props_capnp :
  t -> ?txn:[> `Read | `Write ] Lmdb.Txn.t -> edge_id -> string ->
  ('builder -> unit) ->
  (unit -> 'builder) ->
  ('builder -> 'a Capnp.BytesMessage.Message.t) ->
  unit

(** get edge properties using capnproto reader api with zero-copy from lmdb *)
val get_edge_props_capnp :
  t -> ?txn:[> `Read ] Lmdb.Txn.t -> edge_id -> string ->
  (Capnp.Message.ro Capnp.BytesMessage.Message.t -> 'reader) ->
  ('reader -> 'result) ->
  'result
