(** low-level LMDB store operations *)

open Types

(** create/open a gvecdb database at the given path *)
let create (path : string) : t =
  (* create LMDB environment *)
  let flags = Lmdb.Env.Flags.no_subdir in
  (* we use exactly 7 named LMDB databases: nodes, edges, outbound, inbound,
     intern_forward, intern_reverse, metadata *)
  let max_maps = 7 in
  let map_size = 10 * 1024 * 1024 in
  let env = Lmdb.Env.create Lmdb.Rw ~max_maps ~map_size ~flags path in
  
  (* create/open all required indexes *)
  let nodes = Lmdb.Map.create Lmdb.Map.Nodup ~name:"nodes" 
    ~key:Lmdb.Conv.string ~value:Lmdb.Conv.string env in
  let edges = Lmdb.Map.create Lmdb.Map.Nodup ~name:"edges"
    ~key:Lmdb.Conv.string ~value:Lmdb.Conv.string env in
  let outbound = Lmdb.Map.create Lmdb.Map.Nodup ~name:"outbound"
    ~key:Lmdb.Conv.string ~value:Lmdb.Conv.string env in
  let inbound = Lmdb.Map.create Lmdb.Map.Nodup ~name:"inbound"
    ~key:Lmdb.Conv.string ~value:Lmdb.Conv.string env in
  let intern_forward = Lmdb.Map.create Lmdb.Map.Nodup ~name:"intern_forward"
    ~key:Lmdb.Conv.string ~value:Lmdb.Conv.string env in
  let intern_reverse = Lmdb.Map.create Lmdb.Map.Nodup ~name:"intern_reverse"
    ~key:Lmdb.Conv.string ~value:Lmdb.Conv.string env in
  let metadata = Lmdb.Map.create Lmdb.Map.Nodup ~name:"metadata"
    ~key:Lmdb.Conv.string ~value:Lmdb.Conv.string env in
  
  let db = {
    env; nodes; edges; outbound; inbound;
    intern_forward; intern_reverse; metadata;
  } in
  
  (* initialize metadata if this is a new database *)
  (try
    let _ = Lmdb.Map.get metadata Metadata.version in
    () (* already initialized *)
  with Not_found ->
    (* first time initialization *)
    Lmdb.Map.set metadata Metadata.version (Keys.encode_id db_version);
    Lmdb.Map.set metadata Metadata.next_node_id (Keys.encode_id 0L);
    Lmdb.Map.set metadata Metadata.next_edge_id (Keys.encode_id 0L);
    Lmdb.Map.set metadata Metadata.next_intern_id (Keys.encode_id 0L);
    Lmdb.Map.set metadata Metadata.next_vector_id (Keys.encode_id 0L);
  );
  
  db

(** close the database *)
let close (db : t) : unit =
  Lmdb.Env.close db.env

(** get and increment a metadata counter *)
let get_next_id (db : t) ?txn (key : string) : id =
  let current = Lmdb.Map.get db.metadata ?txn key |> Keys.decode_id in
  let next = Int64.add current 1L in
  Lmdb.Map.set db.metadata ?txn key (Keys.encode_id next);
  current

(** string interning: get or create an ID for a string *)
let intern (db : t) ?txn (s : string) : intern_id =
  try
    (* check if already interned *)
    Lmdb.Map.get db.intern_forward ?txn s |> Keys.decode_id
  with Not_found ->
    (* assign new ID *)
    let new_id = get_next_id db ?txn Metadata.next_intern_id in
    Lmdb.Map.set db.intern_forward ?txn s (Keys.encode_id new_id);
    Lmdb.Map.set db.intern_reverse ?txn (Keys.encode_id new_id) s;
    new_id

(** lookup an interned string ID without creating if not found *)
let lookup_intern (db : t) ?txn (s : string) : intern_id option =
  try
    Some (Lmdb.Map.get db.intern_forward ?txn s |> Keys.decode_id)
  with Not_found -> None

(** reverse string intern lookup: ID -> string *)
let unintern (db : t) ?txn (id : intern_id) : string =
  Lmdb.Map.get db.intern_reverse ?txn (Keys.encode_id id)

(** check if a string is already interned *)
let is_interned (db : t) ?txn (s : string) : bool =
  try
    let _ = Lmdb.Map.get db.intern_forward ?txn s in
    true
  with Not_found -> false

