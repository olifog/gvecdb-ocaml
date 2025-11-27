(** Low-level LMDB store operations *)

open Types

module Bigstring = Bigstringaf

let create (path : string) : t =
  let flags = Lmdb.Env.Flags.no_subdir in
  let max_maps = 9 in
  let map_size = 100 * 1024 * 1024 in
  let env = Lmdb.Env.create Lmdb.Rw ~max_maps ~map_size ~flags path in
  
  let nodes = Lmdb.Map.create Lmdb.Map.Nodup ~name:"nodes" 
    ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env in
  let edges = Lmdb.Map.create Lmdb.Map.Nodup ~name:"edges"
    ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env in
  let node_meta = Lmdb.Map.create Lmdb.Map.Nodup ~name:"node_meta"
    ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env in
  let edge_meta = Lmdb.Map.create Lmdb.Map.Nodup ~name:"edge_meta"
    ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env in
  let outbound = Lmdb.Map.create Lmdb.Map.Nodup ~name:"outbound"
    ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env in
  let inbound = Lmdb.Map.create Lmdb.Map.Nodup ~name:"inbound"
    ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env in
  let intern_forward = Lmdb.Map.create Lmdb.Map.Nodup ~name:"intern_forward"
    ~key:Lmdb.Conv.string ~value:Lmdb.Conv.bigstring env in
  let intern_reverse = Lmdb.Map.create Lmdb.Map.Nodup ~name:"intern_reverse"
    ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.string env in
  let metadata = Lmdb.Map.create Lmdb.Map.Nodup ~name:"metadata"
    ~key:Lmdb.Conv.string ~value:Lmdb.Conv.bigstring env in
  
  let db = {
    env; nodes; edges; node_meta; edge_meta; 
    outbound; inbound; intern_forward; intern_reverse; metadata;
  } in
  
  (try
    let _ = Lmdb.Map.get metadata Metadata.version in
    ()
  with Not_found ->
    Lmdb.Map.set metadata Metadata.version (Keys.encode_id_bs db_version);
    Lmdb.Map.set metadata Metadata.next_node_id (Keys.encode_id_bs 0L);
    Lmdb.Map.set metadata Metadata.next_edge_id (Keys.encode_id_bs 0L);
    Lmdb.Map.set metadata Metadata.next_intern_id (Keys.encode_id_bs 0L);
    Lmdb.Map.set metadata Metadata.next_vector_id (Keys.encode_id_bs 0L);
  );
  
  db

let close (db : t) : unit =
  Lmdb.Env.close db.env

let get_next_id (db : t) ?txn (key : string) : id =
  let current_bs = Lmdb.Map.get db.metadata ?txn key in
  let current = Keys.decode_id_bs current_bs in
  let next = Int64.add current 1L in
  Lmdb.Map.set db.metadata ?txn key (Keys.encode_id_bs next);
  current

let intern (db : t) ?txn (s : string) : intern_id =
  try
    Keys.decode_id_bs (Lmdb.Map.get db.intern_forward ?txn s)
  with Not_found ->
    let new_id = get_next_id db ?txn Metadata.next_intern_id in
    Lmdb.Map.set db.intern_forward ?txn s (Keys.encode_id_bs new_id);
    Lmdb.Map.set db.intern_reverse ?txn (Keys.encode_id_bs new_id) s;
    new_id

let lookup_intern (db : t) ?txn (s : string) : intern_id option =
  try
    Some (Keys.decode_id_bs (Lmdb.Map.get db.intern_forward ?txn s))
  with Not_found -> None

let unintern (db : t) ?txn (id : intern_id) : string =
  Lmdb.Map.get db.intern_reverse ?txn (Keys.encode_id_bs id)

let empty_bigstring : bigstring = Bigstring.create 0
