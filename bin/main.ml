open Lmdb

let () =
  print_endline "Creating LMDB environment..." ;
  let env = Env.(create Rw ~flags:Flags.no_subdir "/tmp/gvecdb_test.db") in

  print_endline "Creating map..." ;
  let map =
    Map.(create Nodup ~key:Conv.string ~value:Conv.string) env in

  print_endline "Setting key-value pairs..." ;
  Map.set map "hello" "world" ;
  Map.set map "foo" "bar" ;

  print_endline "Reading values back..." ;
  let v1 = Map.get map "hello" in
  let v2 = Map.get map "foo" in
  
  Printf.printf "hello -> %s\n" v1 ;
  Printf.printf "foo -> %s\n" v2 ;
  
  print_endline "LMDB test completed successfully!"
