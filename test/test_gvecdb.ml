open Lmdb

let test_basic_operations () =
  print_endline "Test: Basic LMDB operations" ;
  
  let env = Env.(create Rw ~flags:Flags.no_subdir "/tmp/gvecdb_test_suite.db") in
  let map = Map.(create Nodup ~key:Conv.string ~value:Conv.string) env in
  
  (* Test set and get *)
  Map.set map "key1" "value1" ;
  let result = Map.get map "key1" in
  assert (result = "value1") ;
  print_endline "  ✓ Set and get working" ;
  
  (* Test multiple keys *)
  Map.set map "key2" "value2" ;
  Map.set map "key3" "value3" ;
  assert (Map.get map "key2" = "value2") ;
  assert (Map.get map "key3" = "value3") ;
  print_endline "  ✓ Multiple keys working" ;
  
  (* Test overwrite *)
  Map.set map "key1" "new_value1" ;
  assert (Map.get map "key1" = "new_value1") ;
  print_endline "  ✓ Overwrite working" ;
  
  print_endline "All tests passed!"

let () =
  test_basic_operations ()

