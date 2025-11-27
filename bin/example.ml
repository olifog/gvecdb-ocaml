(** example client demonstrating gvecdb usage with CapnProto schemas - DIRECT API *)

module SchemaMod = Schemas.Make(Capnp.BytesMessage)

let () =
  print_endline "=== gvecdb example client ===" ;
  print_endline "" ;
  
  print_endline "creating database at /tmp/gvecdb_example.db" ;
  let db = Gvecdb.create "/tmp/gvecdb_example.db" in
  print_endline "" ;
  
  print_endline "creating person nodes" ;
  let alice = Gvecdb.create_node db "person" in
  let bob = Gvecdb.create_node db "person" in
  let charlie = Gvecdb.create_node db "person" in
  
  Printf.printf "  alice (node ID): %Ld\n" alice ;
  Printf.printf "  bob (node ID): %Ld\n" bob ;
  Printf.printf "  charlie (node ID): %Ld\n" charlie ;
  print_endline "" ;
  
  print_endline "creating edges" ;
  let edge1 = Gvecdb.create_edge db "knows" alice bob in
  let edge2 = Gvecdb.create_edge db "knows" bob charlie in
  let edge3 = Gvecdb.create_edge db "likes" alice charlie in
  
  Printf.printf "  alice --[knows]--> bob (edge ID): %Ld\n" edge1 ;
  Printf.printf "  bob --[knows]--> charlie (edge ID): %Ld\n" edge2 ;
  Printf.printf "  alice --[likes]--> charlie (edge ID): %Ld\n" edge3 ;
  print_endline "" ;
  
  print_endline "testing adjacency queries" ;
  let alice_outbound = Gvecdb.get_outbound_edges db alice in
  Printf.printf "  alice outbound edges (%d):\n" (List.length alice_outbound) ;
  List.iter (fun edge ->
    Printf.printf "    edge %Ld: [%s] -> node %Ld\n" 
      edge.Gvecdb.id edge.Gvecdb.edge_type edge.Gvecdb.dst
  ) alice_outbound ;
  print_endline "" ;
  
  print_endline "registering schemas (just metadata for validation)" ;
  Gvecdb.register_node_schema_capnp db "person" 0xd8e6e025e7838111L;
  Gvecdb.register_edge_schema_capnp db "knows" 0xd3c22e2de1d0b32bL;
  print_endline "schemas registered" ;
  print_endline "" ;
  
  print_endline "setting alice's properties using capnproto builder api" ;
  Gvecdb.set_node_props_capnp db alice "person"
    (fun builder ->
      SchemaMod.Builder.Person.name_set builder "Alice Smith";
      SchemaMod.Builder.Person.age_set_int_exn builder 30;
      SchemaMod.Builder.Person.email_set builder "alice@example.com";
      SchemaMod.Builder.Person.bio_set builder "Software engineer"
    )
    SchemaMod.Builder.Person.init_root
    SchemaMod.Builder.Person.to_message;
  print_endline "properties set" ;
  print_endline "" ;
  
  print_endline "reading alice's name using capnproto reader api" ;
  let alice_name = Gvecdb.get_node_props_capnp db alice "person"
    SchemaMod.Reader.Person.of_message
    SchemaMod.Reader.Person.name_get in
  Printf.printf "  name: %s\n" alice_name;
  print_endline "" ;
  
  print_endline "reading alice's full properties" ;
  let (alice_name, alice_age, alice_email, alice_bio) = 
    Gvecdb.get_node_props_capnp db alice "person"
      SchemaMod.Reader.Person.of_message
      (fun reader ->
        let name = SchemaMod.Reader.Person.name_get reader in
        let age = SchemaMod.Reader.Person.age_get_int_exn reader in
        let email = SchemaMod.Reader.Person.email_get reader in
        let bio = SchemaMod.Reader.Person.bio_get reader in
        (name, age, email, bio)
      )
  in
  Printf.printf "  name: %s\n" alice_name;
  Printf.printf "  age: %d\n" alice_age;
  Printf.printf "  email: %s\n" alice_email;
  Printf.printf "  bio: %s\n" alice_bio;
  print_endline "" ;
  
  print_endline "setting edge properties using capnproto builder api" ;
  Gvecdb.set_edge_props_capnp db edge1 "knows"
    (fun builder ->
      SchemaMod.Builder.Knows.since_set builder 1609459200L;
      SchemaMod.Builder.Knows.strength_set builder 0.85;
      SchemaMod.Builder.Knows.context_set builder "Met at university";
      SchemaMod.Builder.Knows.last_contact_set builder 1700000000L
    )
    SchemaMod.Builder.Knows.init_root
    SchemaMod.Builder.Knows.to_message;
  print_endline "edge properties set" ;
  print_endline "" ;
  
  print_endline "reading edge properties using capnproto reader api" ;
  let (since, strength, context) = 
    Gvecdb.get_edge_props_capnp db edge1 "knows"
      SchemaMod.Reader.Knows.of_message
      (fun reader ->
        let since = SchemaMod.Reader.Knows.since_get reader in
        let strength = SchemaMod.Reader.Knows.strength_get reader in
        let context = SchemaMod.Reader.Knows.context_get reader in
        (since, strength, context)
      )
  in
  Printf.printf "  since: %Ld\n" since;
  Printf.printf "  strength: %.2f\n" strength;
  Printf.printf "  context: %s\n" context;
  print_endline "" ;
    
  (* get_node_info *)
  print_endline "  getting node info for alice" ;
  (match Gvecdb.get_node_info db alice with
   | Some info ->
       Printf.printf "    node %Ld has type: %s\n" info.Gvecdb.id info.Gvecdb.node_type
   | None -> print_endline "    node not found");
  
  (* delete an edge *)
  print_endline "  deleting edge: alice --[likes]--> charlie" ;
  Gvecdb.delete_edge db edge3;
  let alice_outbound_after = Gvecdb.get_outbound_edges db alice in
  Printf.printf "    alice now has %d outbound edges (down from 2)\n" 
    (List.length alice_outbound_after);
  
  (* delete a node *)
  print_endline "  deleting node: charlie" ;
  Gvecdb.delete_node db charlie;
  Printf.printf "    charlie exists: %b\n" (Gvecdb.node_exists db charlie);
  print_endline "" ;
  
  (* transaction example *)
  print_endline "=== transaction examples ===" ;
  print_endline "" ;
  
  print_endline "creating multiple nodes atomically with with_transaction" ;
  let result = Gvecdb.with_transaction db (fun txn ->
    (* all these operations are atomic *)
    let dave = Gvecdb.create_node db ~txn "person" in
    let eve = Gvecdb.create_node db ~txn "person" in
    let frank = Gvecdb.create_node db ~txn "person" in
    
    (* create edges within the same transaction *)
    let _ = Gvecdb.create_edge db ~txn "knows" dave eve in
    let _ = Gvecdb.create_edge db ~txn "knows" eve frank in
    let _ = Gvecdb.create_edge db ~txn "knows" dave frank in
    
    (* set properties within the transaction *)
    Gvecdb.set_node_props_capnp db ~txn dave "person"
      (fun builder ->
        SchemaMod.Builder.Person.name_set builder "Dave";
        SchemaMod.Builder.Person.age_set_int_exn builder 25)
      SchemaMod.Builder.Person.init_root
      SchemaMod.Builder.Person.to_message;
    
    (dave, eve, frank)
  ) in
  (match result with
   | Some (dave, eve, frank) ->
       Printf.printf "  transaction committed! created nodes: %Ld, %Ld, %Ld\n" dave eve frank;
       Printf.printf "  dave's outbound edges: %d\n" 
         (List.length (Gvecdb.get_outbound_edges db dave))
   | None ->
       print_endline "  transaction aborted!");
  print_endline "" ;
  
  print_endline "demonstrating transaction rollback on exception" ;
  let node_count_before = 
    (* count nodes by checking existence up to ID 100 *)
    let count = ref 0 in
    for i = 0 to 100 do
      if Gvecdb.node_exists db (Int64.of_int i) then incr count
    done;
    !count
  in
  Printf.printf "  nodes before failed transaction: %d\n" node_count_before;
  
  (try
    let _ = Gvecdb.with_transaction db (fun txn ->
      let _ = Gvecdb.create_node db ~txn "person" in
      let _ = Gvecdb.create_node db ~txn "person" in
      (* simulate an error - transaction will be rolled back *)
      failwith "simulated error!"
    ) in
    ()
  with Failure _ ->
    print_endline "  exception caught, transaction rolled back");
  
  let node_count_after = 
    let count = ref 0 in
    for i = 0 to 100 do
      if Gvecdb.node_exists db (Int64.of_int i) then incr count
    done;
    !count
  in
  Printf.printf "  nodes after failed transaction: %d (unchanged!)\n" node_count_after;
  print_endline "" ;
  
  print_endline "closing database" ;
  Gvecdb.close db ;
  print_endline "" ;
  print_endline "example completed successfully!"
