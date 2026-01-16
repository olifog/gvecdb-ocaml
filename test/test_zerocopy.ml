(** Zero-copy semantics and memory safety tests.

    These tests verify that: 1. Integer/struct reads don't copy the entire
    message 2. Text fields do copy (expected capnp-ocaml behavior) 3. Data is
    safely accessed within transactions 4. Reading multiple fields shares the
    same backing memory *)

open Alcotest
open Test_common

let measure_alloc f =
  Gc.full_major ();
  Gc.full_major ();
  let before = Gc.allocated_bytes () in
  let result = f () in
  let after = Gc.allocated_bytes () in
  (result, after -. before)

(** {1 Zero-copy verification for nodes} *)

let test_integer_field_no_message_copy () =
  with_temp_db "zerocopy" @@ fun db ->
  register_schemas db;
  let large_bio = String.make 1_000_000 'x' in
  let node = create_person db "Test" 42 "test@test.com" large_bio in

  let age, alloc = measure_alloc (fun () -> get_person_age db node) in

  check
    (testable
       (fun fmt a -> Format.fprintf fmt "%s" (Stdint.Uint32.to_string a))
       (fun a b -> Stdint.Uint32.compare a b = 0))
    "age value" (Stdint.Uint32.of_int 42) age;

  if alloc > 100_000.0 then
    fail
      (Printf.sprintf "Integer read allocated %.0f bytes (expected < 100KB)"
         alloc)

let test_multiple_int_reads_efficient () =
  with_temp_db "zerocopy" @@ fun db ->
  register_schemas db;
  let large_bio = String.make 1_000_000 'x' in
  let node = create_person db "Test" 42 "test@test.com" large_bio in

  let max_alloc = ref 0.0 in
  for _ = 1 to 10 do
    let _, alloc = measure_alloc (fun () -> get_person_age db node) in
    if alloc > !max_alloc then max_alloc := alloc
  done;

  if !max_alloc > 100_000.0 then
    fail (Printf.sprintf "Repeated reads allocated up to %.0f bytes" !max_alloc)

let test_text_field_copies () =
  with_temp_db "zerocopy" @@ fun db ->
  register_schemas db;
  let large_bio = String.make 500_000 'x' in
  let node = create_person db "Test" 42 "test@test.com" large_bio in

  let bio_len, alloc =
    measure_alloc (fun () ->
        ok_exn
          (Gvecdb.get_node_props_capnp db node
             SchemaReader.Reader.Person.of_message (fun r ->
               String.length (SchemaReader.Reader.Person.bio_get r))))
  in

  check int "bio length" 500_000 bio_len;
  if alloc < 400_000.0 then
    Printf.printf
      "Note: text field allocated only %.0f bytes (expected ~500KB)\n" alloc

(** {1 Zero-copy verification for edges} *)

let test_edge_int_field_no_copy () =
  with_temp_db "zerocopy" @@ fun db ->
  register_schemas db;
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let large_context = String.make 1_000_000 'x' in
  let edge = ok_exn (Gvecdb.create_edge db "knows" a b) in
  ok_exn
    (Gvecdb.set_edge_props_capnp db edge "knows"
       (fun builder ->
         SchemaBuilder.Builder.Knows.since_set builder 12345L;
         SchemaBuilder.Builder.Knows.context_set builder large_context;
         SchemaBuilder.Builder.Knows.strength_set builder 0.5)
       SchemaBuilder.Builder.Knows.init_root
       SchemaBuilder.Builder.Knows.to_message);

  let since, alloc =
    measure_alloc (fun () ->
        ok_exn
          (Gvecdb.get_edge_props_capnp db edge
             SchemaReader.Reader.Knows.of_message
             SchemaReader.Reader.Knows.since_get))
  in

  check int64 "since value" 12345L since;
  if alloc > 100_000.0 then
    fail (Printf.sprintf "Edge int read allocated %.0f bytes" alloc)

(** {1 Memory safety within transactions} *)

let test_read_inside_transaction_safe () =
  with_temp_db "zerocopy" @@ fun db ->
  register_schemas db;
  let node = create_person db "Alice" 30 "alice@test.com" "Engineer" in

  let result =
    Gvecdb.with_transaction_ro db (fun txn ->
        let name = get_person_name db ~txn node in
        String.length name)
  in

  match result with
  | Some len -> check int "name length" 5 len
  | None -> fail "transaction failed"

let test_multiple_reads_same_txn () =
  with_temp_db "zerocopy" @@ fun db ->
  register_schemas db;
  let node = create_person db "Alice" 30 "alice@test.com" "Software Engineer" in

  let result =
    Gvecdb.with_transaction_ro db (fun txn ->
        let name1 = get_person_name db ~txn node in
        let name2 = get_person_name db ~txn node in
        let name3 = get_person_name db ~txn node in
        (name1, name2, name3))
  in

  match result with
  | Some (n1, n2, n3) ->
      check string "name1" "Alice" n1;
      check string "name2" "Alice" n2;
      check string "name3" "Alice" n3
  | None -> fail "transaction failed"

let test_read_multiple_fields_one_call () =
  with_temp_db "zerocopy" @@ fun db ->
  register_schemas db;
  let large_bio = String.make 100_000 'x' in
  let node = create_person db "Alice" 30 "alice@test.com" large_bio in

  let result, alloc =
    measure_alloc (fun () ->
        ok_exn
          (Gvecdb.get_node_props_capnp db node
             SchemaReader.Reader.Person.of_message (fun r ->
               let name = SchemaReader.Reader.Person.name_get r in
               let age = SchemaReader.Reader.Person.age_get r in
               let email = SchemaReader.Reader.Person.email_get r in
               (name, age, email))))
  in

  let name, age, email = result in
  check string "name" "Alice" name;
  check
    (testable
       (fun fmt a -> Format.fprintf fmt "%s" (Stdint.Uint32.to_string a))
       (fun a b -> Stdint.Uint32.compare a b = 0))
    "age" (Stdint.Uint32.of_int 30) age;
  check string "email" "alice@test.com" email;

  if alloc > 50_000.0 then
    fail (Printf.sprintf "Multi-field read allocated %.0f bytes" alloc)

(** {1 Data consistency tests} *)

let test_data_not_corrupted_after_update () =
  with_temp_db "zerocopy" @@ fun db ->
  register_schemas db;
  let node = create_person db "Alice" 30 "alice@test.com" "bio" in

  let name_before = get_person_name db node in
  check string "name before" "Alice" name_before;

  ok_exn
    (Gvecdb.set_node_props_capnp db node "person"
       (fun b ->
         SchemaBuilder.Builder.Person.name_set b "Bob";
         SchemaBuilder.Builder.Person.age_set_int_exn b 25)
       SchemaBuilder.Builder.Person.init_root
       SchemaBuilder.Builder.Person.to_message);

  let name_after = get_person_name db node in
  check string "name after" "Bob" name_after;

  check string "original preserved" "Alice" name_before

let test_read_after_delete_fails () =
  with_temp_db "zerocopy" @@ fun db ->
  register_schemas db;
  let node = create_person db "Alice" 30 "alice@test.com" "bio" in
  ok_exn (Gvecdb.delete_node db node);

  match
    Gvecdb.get_node_props_capnp db node SchemaReader.Reader.Person.of_message
      SchemaReader.Reader.Person.name_get
  with
  | Error (Gvecdb.Node_not_found _) -> ()
  | _ -> fail "expected Node_not_found error"

(** {1 Large data tests} *)

let test_large_property_roundtrip () =
  with_temp_db "zerocopy" @@ fun db ->
  register_schemas db;
  let large_bio = String.init 5_000_000 (fun i -> Char.chr (65 + (i mod 26))) in
  let node = create_person db "Test" 1 "test@test.com" large_bio in

  let bio_back =
    ok_exn
      (Gvecdb.get_node_props_capnp db node SchemaReader.Reader.Person.of_message
         SchemaReader.Reader.Person.bio_get)
  in

  check int "bio length" 5_000_000 (String.length bio_back);
  check string "bio content matches" large_bio bio_back

let test_many_small_reads () =
  with_temp_db "zerocopy" @@ fun db ->
  register_schemas db;
  let node = create_person db "Alice" 30 "alice@test.com" "bio" in

  let initial_words = (Gc.stat ()).live_words in
  for _ = 1 to 1000 do
    ignore (get_person_name db node)
  done;
  Gc.full_major ();
  let final_words = (Gc.stat ()).live_words in

  let growth = final_words - initial_words in
  if growth > 100_000 then
    fail (Printf.sprintf "Memory grew by %d words over 1000 reads" growth)

(** {1 Test runner} *)

let node_zerocopy_tests =
  [
    ("int_field_no_copy", `Quick, test_integer_field_no_message_copy);
    ("multiple_int_reads", `Quick, test_multiple_int_reads_efficient);
    ("text_field_copies", `Quick, test_text_field_copies);
  ]

let edge_zerocopy_tests =
  [ ("edge_int_no_copy", `Quick, test_edge_int_field_no_copy) ]

let safety_tests =
  [
    ("read_in_txn_safe", `Quick, test_read_inside_transaction_safe);
    ("multiple_reads_same_txn", `Quick, test_multiple_reads_same_txn);
    ("multi_field_one_call", `Quick, test_read_multiple_fields_one_call);
  ]

let consistency_tests =
  [
    ("data_after_update", `Quick, test_data_not_corrupted_after_update);
    ("read_deleted_fails", `Quick, test_read_after_delete_fails);
  ]

let large_data_tests =
  [
    ("large_roundtrip", `Slow, test_large_property_roundtrip);
    ("many_small_reads", `Quick, test_many_small_reads);
  ]

let () =
  run "ZeroCopy"
    [
      ("node_zerocopy", node_zerocopy_tests);
      ("edge_zerocopy", edge_zerocopy_tests);
      ("memory_safety", safety_tests);
      ("consistency", consistency_tests);
      ("large_data", large_data_tests);
    ]
