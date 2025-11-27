(** Core type definitions for gvecdb *)

(** 64-bit IDs for nodes, edges, vectors, etc. *)
type id = int64

(** IDs for interned strings (node types, edge types, etc.) *)
type intern_id = int64

(** node ID *)
type node_id = id

(** edge ID *)
type edge_id = id

(** vector ID *)
type vector_id = id

(** node information record *)
type node_info = {
  id : node_id;
  node_type : string;
}

(** edge information record *)
type edge_info = {
  id : edge_id;
  edge_type : string;
  src : node_id;
  dst : node_id;
}

(** transaction handle - wraps LMDB transaction *)
type 'perm txn = 'perm Lmdb.Txn.t constraint 'perm = [< `Read | `Write ]

(** read-only transaction *)
type ro_txn = [ `Read ] txn

(** read-write transaction *)
type rw_txn = [ `Read | `Write ] txn

(** database handle *)
type t = {
  env : Lmdb.Env.t;
  (* core indexes *)
  nodes : (string, string, [`Uni]) Lmdb.Map.t;              (* node_id -> node data blob *)
  edges : (string, string, [`Uni]) Lmdb.Map.t;              (* edge_id -> edge data blob *)
  outbound : (string, string, [`Uni]) Lmdb.Map.t;           (* (src, type, dst, edge_id) -> () *)
  inbound : (string, string, [`Uni]) Lmdb.Map.t;            (* (dst, type, src, edge_id) -> () *)
  
  (* string interning indexes *)
  intern_forward : (string, string, [`Uni]) Lmdb.Map.t;     (* string -> id *)
  intern_reverse : (string, string, [`Uni]) Lmdb.Map.t;     (* id -> string *)
  
  (* metadata *)
  metadata : (string, string, [`Uni]) Lmdb.Map.t;           (* metadata_key -> value *)
}

(** metadata keys for tracking database state *)
module Metadata = struct
  let version = "version"
  let next_node_id = "next_node_id"
  let next_edge_id = "next_edge_id"
  let next_intern_id = "next_intern_id"
  let next_vector_id = "next_vector_id"
end

(** current database version *)
let db_version = 1L

(** [with_transaction db f] runs [f] within a read-write transaction.
    
    All operations within [f] will be atomic - either all succeed or all are rolled back.
    The transaction is automatically committed when [f] returns normally.
    If [f] raises an exception, the transaction is rolled back.
    
    @return [Some result] if transaction committed successfully, [None] if aborted via [abort_transaction]
*)
let with_transaction (db : t) (f : rw_txn -> 'a) : 'a option =
  Lmdb.Txn.go Lmdb.Rw db.env f

(** [with_transaction_ro db f] runs [f] within a read-only transaction.
    
    Read-only transactions provide a consistent snapshot view of the database.
    Multiple read-only transactions can run concurrently.
    
    @return [Some result] if transaction completed successfully, [None] if aborted
*)
let with_transaction_ro (db : t) (f : ro_txn -> 'a) : 'a option =
  Lmdb.Txn.go Lmdb.Ro db.env f

(** [abort_transaction txn] aborts the current transaction.
    
    The enclosing [with_transaction] or [with_transaction_ro] will return [None].
*)
let abort_transaction : 'perm txn -> 'a = Lmdb.Txn.abort
