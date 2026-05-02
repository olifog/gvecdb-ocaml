open Eio.Std
module Vat = Capnp_rpc_unix.Vat

let serve db_path map_size config =
  let db =
    match Gvecdb.create ?map_size db_path with
    | Ok db -> db
    | Error e -> failwith (Gvecdb.Error.to_string e)
  in
  Gvecdb.load_all_schemas db;
  Switch.run @@ fun sw ->
  let service = Gvecdb_rpc.Gvecdb_service.local db in
  let service_id = Capnp_rpc_net.Restorer.Id.public "" in
  let restore = Capnp_rpc_net.Restorer.single service_id service in
  let vat = Capnp_rpc_unix.serve ~sw ~restore config in
  (match Capnp_rpc_unix.Cap_file.save_service vat service_id "gvecdb.cap" with
  | Error (`Msg m) -> traceln "Warning: could not save cap file: %s" m
  | Ok () -> traceln "Saved capability to gvecdb.cap");
  traceln "gvecdb server running";
  traceln "Database: %s" db_path;
  Fiber.await_cancel ()

open Cmdliner

let db_path =
  let doc = "Path to the database file" in
  Arg.(required @@ opt (some string) None @@ info [ "db" ] ~docv:"PATH" ~doc)

let map_size =
  let doc = "LMDB map size in bytes (default: 10 GiB)" in
  Arg.(value @@ opt (some int) None @@ info [ "map-size" ] ~docv:"BYTES" ~doc)

let serve_cmd env =
  let doc = "Run a gvecdb Cap'n Proto RPC server" in
  let info = Cmd.info "gvecdb-server" ~version:"0.1.0" ~doc in
  let capnp_env =
    object
      method net = env#net
      method fs = env#fs
    end
  in
  Cmd.v info
    Term.(
      const serve $ db_path $ map_size $ Capnp_rpc_unix.Vat_config.cmd capnp_env)

let () = Eio_main.run @@ fun env -> exit @@ Cmd.eval (serve_cmd env)
