(** example client demonstrating gvecdb usage with CapnProto schemas *)

(* BytesMessage for building, Bigstring_message for reading *)
module SchemaBuilder = Schemas.Make (Capnp.BytesMessage)
module SchemaReader = Schemas.Make (Gvecdb.Bigstring_message)

let ok_exn = function
  | Ok x -> x
  | Error e -> failwith (Gvecdb.Error.to_string e)

let () =
  print_endline "=== gvecdb example client ===";
  print_endline "";

  print_endline "creating database at /tmp/gvecdb_example.db";
  let db = ok_exn (Gvecdb.create "/tmp/gvecdb_example.db") in
  print_endline "";

  print_endline "creating person nodes";
  let alice = ok_exn (Gvecdb.create_node db "person") in
  let bob = ok_exn (Gvecdb.create_node db "person") in
  let charlie = ok_exn (Gvecdb.create_node db "person") in

  Printf.printf "  alice (node ID): %Ld\n" alice;
  Printf.printf "  bob (node ID): %Ld\n" bob;
  Printf.printf "  charlie (node ID): %Ld\n" charlie;
  print_endline "";

  print_endline "creating edges";
  let edge1 = ok_exn (Gvecdb.create_edge db "knows" alice bob) in
  let edge2 = ok_exn (Gvecdb.create_edge db "knows" bob charlie) in
  let edge3 = ok_exn (Gvecdb.create_edge db "likes" alice charlie) in

  Printf.printf "  alice --[knows]--> bob (edge ID): %Ld\n" edge1;
  Printf.printf "  bob --[knows]--> charlie (edge ID): %Ld\n" edge2;
  Printf.printf "  alice --[likes]--> charlie (edge ID): %Ld\n" edge3;
  print_endline "";

  print_endline "testing adjacency queries";
  let alice_outbound = ok_exn (Gvecdb.get_outbound_edges db alice) in
  Printf.printf "  alice outbound edges (%d):\n" (List.length alice_outbound);
  List.iter
    (fun edge ->
      Printf.printf "    edge %Ld: [%s] -> node %Ld\n" edge.Gvecdb.id
        edge.Gvecdb.edge_type edge.Gvecdb.dst)
    alice_outbound;
  print_endline "";

  print_endline "registering schemas";
  ok_exn (Gvecdb.register_node_schema_capnp db "person" 0xd8e6e025e7838111L);
  ok_exn (Gvecdb.register_edge_schema_capnp db "knows" 0xd3c22e2de1d0b32bL);
  print_endline "schemas registered";
  print_endline "";

  print_endline "setting alice's properties";
  ok_exn
    (Gvecdb.set_node_props_capnp db alice "person"
       (fun builder ->
         SchemaBuilder.Builder.Person.name_set builder "Alice Smith";
         SchemaBuilder.Builder.Person.age_set_int_exn builder 30;
         SchemaBuilder.Builder.Person.email_set builder "alice@example.com";
         SchemaBuilder.Builder.Person.bio_set builder "Software engineer")
       SchemaBuilder.Builder.Person.init_root
       SchemaBuilder.Builder.Person.to_message);
  print_endline "properties set";
  print_endline "";

  print_endline "reading alice's name";
  let alice_name =
    ok_exn
      (Gvecdb.get_node_props_capnp db alice
         SchemaReader.Reader.Person.of_message
         SchemaReader.Reader.Person.name_get)
  in
  Printf.printf "  name: %s\n" alice_name;
  print_endline "";

  print_endline "reading alice's full properties";
  let alice_name, alice_age, alice_email, alice_bio =
    ok_exn
      (Gvecdb.get_node_props_capnp db alice
         SchemaReader.Reader.Person.of_message (fun reader ->
           let name = SchemaReader.Reader.Person.name_get reader in
           let age = SchemaReader.Reader.Person.age_get_int_exn reader in
           let email = SchemaReader.Reader.Person.email_get reader in
           let bio = SchemaReader.Reader.Person.bio_get reader in
           (name, age, email, bio)))
  in
  Printf.printf "  name: %s\n" alice_name;
  Printf.printf "  age: %d\n" alice_age;
  Printf.printf "  email: %s\n" alice_email;
  Printf.printf "  bio: %s\n" alice_bio;
  print_endline "";

  print_endline "setting edge properties";
  ok_exn
    (Gvecdb.set_edge_props_capnp db edge1 "knows"
       (fun builder ->
         SchemaBuilder.Builder.Knows.since_set builder 1609459200L;
         SchemaBuilder.Builder.Knows.strength_set builder 0.85;
         SchemaBuilder.Builder.Knows.context_set builder "Met at university";
         SchemaBuilder.Builder.Knows.last_contact_set builder 1700000000L)
       SchemaBuilder.Builder.Knows.init_root
       SchemaBuilder.Builder.Knows.to_message);
  print_endline "edge properties set";
  print_endline "";

  print_endline "reading edge properties";
  let since, strength, context =
    ok_exn
      (Gvecdb.get_edge_props_capnp db edge1 SchemaReader.Reader.Knows.of_message
         (fun reader ->
           let since = SchemaReader.Reader.Knows.since_get reader in
           let strength = SchemaReader.Reader.Knows.strength_get reader in
           let context = SchemaReader.Reader.Knows.context_get reader in
           (since, strength, context)))
  in
  Printf.printf "  since: %Ld\n" since;
  Printf.printf "  strength: %.2f\n" strength;
  Printf.printf "  context: %s\n" context;
  print_endline "";

  (* get_node_info *)
  print_endline "  getting node info for alice";
  (match Gvecdb.get_node_info db alice with
  | Ok info ->
      Printf.printf "  node %Ld has type: %s\n" info.Gvecdb.id
        info.Gvecdb.node_type
  | Error _ -> print_endline "  node not found");

  (* delete an edge *)
  print_endline "  deleting edge: alice --[likes]--> charlie";
  ok_exn (Gvecdb.delete_edge db edge3);
  let alice_outbound_after = ok_exn (Gvecdb.get_outbound_edges db alice) in
  Printf.printf "  alice now has %d outbound edges (down from 2)\n"
    (List.length alice_outbound_after);

  (* delete a node *)
  print_endline "  deleting node: charlie";
  ok_exn (Gvecdb.delete_node db charlie);
  Printf.printf "  charlie exists: %b\n"
    (ok_exn (Gvecdb.node_exists db charlie));
  print_endline "";

  (* transaction example *)
  print_endline "=== transaction examples ===";
  print_endline "";

  print_endline "creating multiple nodes atomically";
  let result =
    Gvecdb.with_transaction db (fun txn ->
        let dave = ok_exn (Gvecdb.create_node db ~txn "person") in
        let eve = ok_exn (Gvecdb.create_node db ~txn "person") in
        let frank = ok_exn (Gvecdb.create_node db ~txn "person") in

        let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" dave eve) in
        let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" eve frank) in
        let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" dave frank) in

        ok_exn
          (Gvecdb.set_node_props_capnp db ~txn dave "person"
             (fun builder ->
               SchemaBuilder.Builder.Person.name_set builder "Dave";
               SchemaBuilder.Builder.Person.age_set_int_exn builder 25)
             SchemaBuilder.Builder.Person.init_root
             SchemaBuilder.Builder.Person.to_message);

        (dave, eve, frank))
  in
  (match result with
  | Some (dave, eve, frank) ->
      Printf.printf "  transaction committed! created nodes: %Ld, %Ld, %Ld\n"
        dave eve frank;
      Printf.printf "  dave's outbound edges: %d\n"
        (List.length (ok_exn (Gvecdb.get_outbound_edges db dave)))
  | None -> print_endline "  transaction aborted!");
  print_endline "";

  print_endline "demonstrating transaction rollback on exception";
  let node_count_before =
    let count = ref 0 in
    for i = 0 to 100 do
      if ok_exn (Gvecdb.node_exists db (Int64.of_int i)) then incr count
    done;
    !count
  in
  Printf.printf "  nodes before failed transaction: %d\n" node_count_before;

  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           let _ = ok_exn (Gvecdb.create_node db ~txn "person") in
           let _ = ok_exn (Gvecdb.create_node db ~txn "person") in
           (* simulate an error - transaction will be rolled back *)
           failwith "simulated error!")
     in
     ()
   with Failure _ ->
     print_endline "  exception caught, transaction rolled back");

  let node_count_after =
    let count = ref 0 in
    for i = 0 to 100 do
      if ok_exn (Gvecdb.node_exists db (Int64.of_int i)) then incr count
    done;
    !count
  in
  Printf.printf "  nodes after failed transaction: %d (unchanged!)\n"
    node_count_after;
  print_endline "";

  print_endline "closing database";
  Gvecdb.close db;
  print_endline "";
  print_endline "example completed successfully!"
