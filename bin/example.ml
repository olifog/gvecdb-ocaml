(** example client demonstrating gvecdb usage with CapnProto schemas *)

(* BytesMessage for both building and reading *)
module SchemaBuilder = Schemas.Make (Capnp.BytesMessage)
module SchemaReader = SchemaBuilder

let ok_exn = function
  | Ok x -> x
  | Error e -> failwith (Gvecdb.Error.to_string e)

(** Serialize a capnp builder message to a bigstring (wire format) *)
let capnp_to_bigstring to_message builder =
  let msg = to_message builder in
  let total = ref 0 in
  Capnp.Codecs.serialize_iter msg ~compression:`None ~f:(fun fragment ->
      total := !total + String.length fragment);
  let bs = Bigstringaf.create !total in
  let pos = ref 0 in
  Capnp.Codecs.serialize_iter msg ~compression:`None ~f:(fun fragment ->
      let len = String.length fragment in
      Bigstringaf.blit_from_string fragment ~src_off:0 bs ~dst_off:!pos ~len;
      pos := !pos + len);
  bs

(** Read capnp wire-format props from a bigstring *)
let decode_and_read bs of_message read_fn =
  let s = Bigstringaf.to_string bs in
  let stream = Capnp.Codecs.FramedStream.of_string ~compression:`None s in
  match Capnp.Codecs.FramedStream.get_next_frame stream with
  | Ok msg ->
      let ro_msg = Capnp.BytesMessage.Message.readonly msg in
      let reader = of_message ro_msg in
      read_fn reader
  | Error _ -> failwith "failed to decode capnp wire format"

let read_node_props db node of_message read_fn =
  let bs = ok_exn (Gvecdb.get_node_props db node) in
  decode_and_read bs of_message read_fn

let read_edge_props db edge of_message read_fn =
  let bs = ok_exn (Gvecdb.get_edge_props db edge) in
  decode_and_read bs of_message read_fn

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
  let alice_outbound = ok_exn (Gvecdb.get_outbound_edges db alice ()) in
  Printf.printf "  alice outbound edges (%d):\n" (List.length alice_outbound);
  List.iter
    (fun edge ->
      Printf.printf "    edge %Ld: [%s] -> node %Ld\n" edge.Gvecdb.id
        edge.Gvecdb.edge_type edge.Gvecdb.dst)
    alice_outbound;
  print_endline "";

  print_endline "setting alice's properties";
  let builder = SchemaBuilder.Builder.Person.init_root () in
  SchemaBuilder.Builder.Person.name_set builder "Alice Smith";
  SchemaBuilder.Builder.Person.age_set_int_exn builder 30;
  SchemaBuilder.Builder.Person.email_set builder "alice@example.com";
  SchemaBuilder.Builder.Person.bio_set builder "Software engineer";
  let bs = capnp_to_bigstring SchemaBuilder.Builder.Person.to_message builder in
  ok_exn (Gvecdb.set_node_props db alice "person" bs);
  print_endline "properties set";
  print_endline "";

  print_endline "reading alice's name";
  let alice_name =
    read_node_props db alice
      SchemaReader.Reader.Person.of_message
      SchemaReader.Reader.Person.name_get
  in
  Printf.printf "  name: %s\n" alice_name;
  print_endline "";

  print_endline "reading alice's full properties";
  let alice_name, alice_age, alice_email, alice_bio =
    read_node_props db alice
      SchemaReader.Reader.Person.of_message (fun reader ->
        let name = SchemaReader.Reader.Person.name_get reader in
        let age = SchemaReader.Reader.Person.age_get_int_exn reader in
        let email = SchemaReader.Reader.Person.email_get reader in
        let bio = SchemaReader.Reader.Person.bio_get reader in
        (name, age, email, bio))
  in
  Printf.printf "  name: %s\n" alice_name;
  Printf.printf "  age: %d\n" alice_age;
  Printf.printf "  email: %s\n" alice_email;
  Printf.printf "  bio: %s\n" alice_bio;
  print_endline "";

  print_endline "setting edge properties";
  let edge_builder = SchemaBuilder.Builder.Knows.init_root () in
  SchemaBuilder.Builder.Knows.since_set edge_builder 1609459200L;
  SchemaBuilder.Builder.Knows.strength_set edge_builder 0.85;
  SchemaBuilder.Builder.Knows.context_set edge_builder "Met at university";
  SchemaBuilder.Builder.Knows.last_contact_set edge_builder 1700000000L;
  let edge_bs = capnp_to_bigstring
      SchemaBuilder.Builder.Knows.to_message edge_builder in
  ok_exn (Gvecdb.set_edge_props db edge1 edge_bs);
  print_endline "edge properties set";
  print_endline "";

  print_endline "reading edge properties";
  let since, strength, context =
    read_edge_props db edge1 SchemaReader.Reader.Knows.of_message
      (fun reader ->
        let since = SchemaReader.Reader.Knows.since_get reader in
        let strength = SchemaReader.Reader.Knows.strength_get reader in
        let context = SchemaReader.Reader.Knows.context_get reader in
        (since, strength, context))
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
  let alice_outbound_after = ok_exn (Gvecdb.get_outbound_edges db alice ()) in
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

        let builder = SchemaBuilder.Builder.Person.init_root () in
        SchemaBuilder.Builder.Person.name_set builder "Dave";
        SchemaBuilder.Builder.Person.age_set_int_exn builder 25;
        let bs = capnp_to_bigstring
            SchemaBuilder.Builder.Person.to_message builder in
        ok_exn (Gvecdb.set_node_props db ~txn dave "person" bs);

        (dave, eve, frank))
  in
  (match result with
  | Some (dave, eve, frank) ->
      Printf.printf "  transaction committed! created nodes: %Ld, %Ld, %Ld\n"
        dave eve frank;
      Printf.printf "  dave's outbound edges: %d\n"
        (List.length (ok_exn (Gvecdb.get_outbound_edges db dave ())))
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
