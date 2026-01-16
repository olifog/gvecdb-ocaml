(** Low-level LMDB store operations *)

open Types
module Bigstring = Bigstringaf

let default_map_size = 10 * 1024 * 1024 * 1024

(** Derive the vector file path from the LMDB database path *)
let vector_file_path (lmdb_path : string) : string =
  (* LMDB path is the .db file, vector file is alongside it *)
  let base = Filename.remove_extension lmdb_path in
  base ^ ".vectors"

let create ?(map_size = default_map_size) (path : string) : (t, error) result =
  (* First try to create the vector file *)
  let vec_path = vector_file_path path in
  match Vector_file.create vec_path with
  | Error e -> Error (Storage_error (Vector_file.error_to_string e))
  | Ok vector_file -> (
      match
        wrap_lmdb_exn (fun () ->
            let flags = Lmdb.Env.Flags.no_subdir in
            let max_maps = 12 in
            let env = Lmdb.Env.create Lmdb.Rw ~max_maps ~map_size ~flags path in

            let nodes =
              Lmdb.Map.create Lmdb.Map.Nodup ~name:"nodes"
                ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env
            in
            let edges =
              Lmdb.Map.create Lmdb.Map.Nodup ~name:"edges"
                ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env
            in
            let node_meta =
              Lmdb.Map.create Lmdb.Map.Nodup ~name:"node_meta"
                ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env
            in
            let edge_meta =
              Lmdb.Map.create Lmdb.Map.Nodup ~name:"edge_meta"
                ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env
            in
            let outbound =
              Lmdb.Map.create Lmdb.Map.Nodup ~name:"outbound"
                ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env
            in
            let inbound =
              Lmdb.Map.create Lmdb.Map.Nodup ~name:"inbound"
                ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env
            in
            let intern_forward =
              Lmdb.Map.create Lmdb.Map.Nodup ~name:"intern_forward"
                ~key:Lmdb.Conv.string ~value:Lmdb.Conv.bigstring env
            in
            let intern_reverse =
              Lmdb.Map.create Lmdb.Map.Nodup ~name:"intern_reverse"
                ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.string env
            in
            let metadata =
              Lmdb.Map.create Lmdb.Map.Nodup ~name:"metadata"
                ~key:Lmdb.Conv.string ~value:Lmdb.Conv.bigstring env
            in
            let vector_index =
              Lmdb.Map.create Lmdb.Map.Nodup ~name:"vector_index"
                ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env
            in
            let vector_owners =
              Lmdb.Map.create Lmdb.Map.Nodup ~name:"vector_owners"
                ~key:Lmdb.Conv.bigstring ~value:Lmdb.Conv.bigstring env
            in

            let db =
              {
                env;
                nodes;
                edges;
                node_meta;
                edge_meta;
                outbound;
                inbound;
                intern_forward;
                intern_reverse;
                metadata;
                vector_file;
                vector_index;
                vector_owners;
              }
            in

            (try
               let _ = Lmdb.Map.get metadata Metadata.version in
               ()
             with Not_found | Lmdb.Not_found ->
               Lmdb.Map.set metadata Metadata.version
                 (Keys.encode_id_bs db_version);
               Lmdb.Map.set metadata Metadata.next_node_id
                 (Keys.encode_id_bs 0L);
               Lmdb.Map.set metadata Metadata.next_edge_id
                 (Keys.encode_id_bs 0L);
               Lmdb.Map.set metadata Metadata.next_intern_id
                 (Keys.encode_id_bs 0L);
               Lmdb.Map.set metadata Metadata.next_vector_id
                 (Keys.encode_id_bs 0L));

            db)
      with
      | Ok db -> Ok db
      | Error e ->
          Vector_file.close vector_file;
          Error e)

let close (db : t) : unit =
  Vector_file.close db.vector_file;
  Lmdb.Env.close db.env

let get_next_id (db : t) ?txn (key : string) : id =
  let current_bs = Lmdb.Map.get db.metadata ?txn key in
  let current = Keys.decode_id_bs current_bs in
  let next = Int64.add current 1L in
  Lmdb.Map.set db.metadata ?txn key (Keys.encode_id_bs next);
  current

let intern (db : t) ?txn (s : string) : (intern_id, error) result =
  wrap_lmdb_exn (fun () ->
      try Keys.decode_id_bs (Lmdb.Map.get db.intern_forward ?txn s)
      with Not_found | Lmdb.Not_found ->
        let new_id = get_next_id db ?txn Metadata.next_intern_id in
        Lmdb.Map.set db.intern_forward ?txn s (Keys.encode_id_bs new_id);
        Lmdb.Map.set db.intern_reverse ?txn (Keys.encode_id_bs new_id) s;
        new_id)

let lookup_intern (db : t) ?txn (s : string) : intern_id option =
  try Some (Keys.decode_id_bs (Lmdb.Map.get db.intern_forward ?txn s))
  with Not_found | Lmdb.Not_found | Invalid_argument _ -> None

let unintern (db : t) ?txn (id : intern_id) : string =
  Lmdb.Map.get db.intern_reverse ?txn (Keys.encode_id_bs id)

let empty_bigstring : bigstring = Bigstring.create 0
