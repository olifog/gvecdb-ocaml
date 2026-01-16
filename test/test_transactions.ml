(** Transaction semantics tests - rollback, isolation, concurrent reads *)

open Alcotest
open Test_common

(** {1 Transaction commit/rollback} *)

let test_transaction_commits () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let result =
    Gvecdb.with_transaction db (fun txn ->
        let node = ok_exn (Gvecdb.create_node db ~txn "person") in
        node)
  in
  match result with
  | Some node_id ->
      check bool "node exists after commit" true
        (ok_exn (Gvecdb.node_exists db node_id))
  | None -> fail "transaction should have committed"

let test_transaction_rollback_on_exception () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let node_before = ok_exn (Gvecdb.create_node db "person") in
  let next_expected = Int64.add node_before 1L in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           let _ = ok_exn (Gvecdb.create_node db ~txn "person") in
           let _ = ok_exn (Gvecdb.create_node db ~txn "person") in
           failwith "simulated error")
     in
     ()
   with Failure _ -> ());
  check bool "rolled back node 1" false
    (ok_exn (Gvecdb.node_exists db next_expected));
  check bool "rolled back node 2" false
    (ok_exn (Gvecdb.node_exists db (Int64.add next_expected 1L)))

let test_transaction_abort () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let result =
    Gvecdb.with_transaction db (fun txn ->
        let _ = ok_exn (Gvecdb.create_node db ~txn "person") in
        Gvecdb.abort_transaction txn)
  in
  check (option reject) "returns None on abort" None result

let test_transaction_abort_ro () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let _ = ok_exn (Gvecdb.create_node db "person") in
  let result =
    Gvecdb.with_transaction_ro db (fun txn ->
        let _ = ok_exn (Gvecdb.node_exists db ~txn 0L) in
        Gvecdb.abort_transaction txn)
  in
  check (option reject) "ro abort returns None" None result

(** {1 Edge rollback tests} *)

let test_edge_creation_rollback () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" a b) in
           failwith "abort")
     in
     ()
   with Failure _ -> ());
  check int "no outbound edges after rollback" 0
    (List.length (ok_exn (Gvecdb.get_outbound_edges db a)));
  check int "no inbound edges after rollback" 0
    (List.length (ok_exn (Gvecdb.get_inbound_edges db b)))

let test_multiple_edges_rollback () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let nodes = List.init 5 (fun _ -> ok_exn (Gvecdb.create_node db "person")) in
  let a = List.hd nodes in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           List.iter
             (fun b ->
               if a <> b then
                 let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" a b) in
                 ())
             nodes;
           failwith "abort")
     in
     ()
   with Failure _ -> ());
  check int "no outbound edges after rollback" 0
    (List.length (ok_exn (Gvecdb.get_outbound_edges db a)))

let test_edge_deletion_rollback () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let edge = ok_exn (Gvecdb.create_edge db "knows" a b) in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           ok_exn (Gvecdb.delete_edge db ~txn edge);
           failwith "abort")
     in
     ()
   with Failure _ -> ());
  check bool "edge still exists after rollback" true
    (ok_exn (Gvecdb.edge_exists db edge));
  check int "outbound edge still exists" 1
    (List.length (ok_exn (Gvecdb.get_outbound_edges db a)))

(** {1 Node deletion rollback tests} *)

let test_node_deletion_rollback () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let node = ok_exn (Gvecdb.create_node db "person") in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           ok_exn (Gvecdb.delete_node db ~txn node);
           failwith "abort")
     in
     ()
   with Failure _ -> ());
  check bool "node still exists after rollback" true
    (ok_exn (Gvecdb.node_exists db node))

let test_node_with_props_deletion_rollback () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let node = create_person db "Alice" 30 "alice@test.com" "bio" in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           ok_exn (Gvecdb.delete_node db ~txn node);
           failwith "abort")
     in
     ()
   with Failure _ -> ());
  check bool "node still exists" true (ok_exn (Gvecdb.node_exists db node));
  check string "props still exist" "Alice" (get_person_name db node)

(** {1 Property rollback tests} *)

let test_props_creation_rollback () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let node = ok_exn (Gvecdb.create_node db "person") in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           ok_exn
             (Gvecdb.set_node_props_capnp db ~txn node "person"
                (fun b ->
                  SchemaBuilder.Builder.Person.name_set b "Alice";
                  SchemaBuilder.Builder.Person.age_set_int_exn b 30;
                  SchemaBuilder.Builder.Person.email_set b "alice@test.com";
                  SchemaBuilder.Builder.Person.bio_set b "bio")
                SchemaBuilder.Builder.Person.init_root
                SchemaBuilder.Builder.Person.to_message);
           failwith "abort")
     in
     ()
   with Failure _ -> ());
  check bool "node exists but has no props" true
    (ok_exn (Gvecdb.node_exists db node))

let test_props_update_rollback () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let node = create_person db "Alice" 30 "alice@test.com" "bio" in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           ok_exn
             (Gvecdb.set_node_props_capnp db ~txn node "person"
                (fun b ->
                  SchemaBuilder.Builder.Person.name_set b "Bob";
                  SchemaBuilder.Builder.Person.age_set_int_exn b 99;
                  SchemaBuilder.Builder.Person.email_set b "bob@test.com";
                  SchemaBuilder.Builder.Person.bio_set b "new bio")
                SchemaBuilder.Builder.Person.init_root
                SchemaBuilder.Builder.Person.to_message);
           failwith "abort")
     in
     ()
   with Failure _ -> ());
  check string "name unchanged after rollback" "Alice" (get_person_name db node);
  check int "age unchanged after rollback" 30
    (Stdint.Uint32.to_int (get_person_age db node))

let test_edge_props_rollback () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let a = ok_exn (Gvecdb.create_node db "person") in
  let b = ok_exn (Gvecdb.create_node db "person") in
  let edge = create_knows_edge db a b 2020L "work" 0.5 in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           ok_exn
             (Gvecdb.set_edge_props_capnp db ~txn edge "knows"
                (fun builder ->
                  SchemaBuilder.Builder.Knows.since_set builder 2024L;
                  SchemaBuilder.Builder.Knows.context_set builder "changed";
                  SchemaBuilder.Builder.Knows.strength_set builder 1.0)
                SchemaBuilder.Builder.Knows.init_root
                SchemaBuilder.Builder.Knows.to_message);
           failwith "abort")
     in
     ()
   with Failure _ -> ());
  let since =
    ok_exn
      (Gvecdb.get_edge_props_capnp db edge SchemaReader.Reader.Knows.of_message
         SchemaReader.Reader.Knows.since_get)
  in
  check Alcotest.int64 "edge props unchanged after rollback" 2020L since

(** {1 Complex rollback scenarios} *)

let test_complex_graph_rollback () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let alice = create_person db "Alice" 30 "alice@test.com" "bio" in
  let bob = create_person db "Bob" 25 "bob@test.com" "bio" in
  let _ = create_knows_edge db alice bob 2020L "work" 0.5 in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           let charlie = ok_exn (Gvecdb.create_node db ~txn "person") in
           let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" bob charlie) in
           let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" alice charlie) in
           ok_exn
             (Gvecdb.set_node_props_capnp db ~txn alice "person"
                (fun b ->
                  SchemaBuilder.Builder.Person.name_set b "Alice Modified";
                  SchemaBuilder.Builder.Person.age_set_int_exn b 31;
                  SchemaBuilder.Builder.Person.email_set b "alice2@test.com";
                  SchemaBuilder.Builder.Person.bio_set b "new bio")
                SchemaBuilder.Builder.Person.init_root
                SchemaBuilder.Builder.Person.to_message);
           failwith "abort")
     in
     ()
   with Failure _ -> ());
  check string "alice name unchanged" "Alice" (get_person_name db alice);
  check int "alice has 1 outbound" 1
    (List.length (ok_exn (Gvecdb.get_outbound_edges db alice)));
  check int "bob has 0 outbound" 0
    (List.length (ok_exn (Gvecdb.get_outbound_edges db bob)))

let test_partial_operations_rollback () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let initial_node = ok_exn (Gvecdb.create_node db "person") in
  let next_id = Int64.add initial_node 1L in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           for _ = 1 to 3 do
             let _ = ok_exn (Gvecdb.create_node db ~txn "person") in
             ()
           done;
           failwith "abort")
     in
     ()
   with Failure _ -> ());
  check bool "node 1 not exists" false (ok_exn (Gvecdb.node_exists db next_id));
  check bool "node 2 not exists" false
    (ok_exn (Gvecdb.node_exists db (Int64.add next_id 1L)));
  check bool "node 3 not exists" false
    (ok_exn (Gvecdb.node_exists db (Int64.add next_id 2L)))

(** {1 Transaction isolation} *)

let test_read_own_writes () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let result =
    Gvecdb.with_transaction db (fun txn ->
        let node = ok_exn (Gvecdb.create_node db ~txn "person") in
        let exists = ok_exn (Gvecdb.node_exists db ~txn node) in
        (node, exists))
  in
  match result with
  | Some (_, exists) -> check bool "can read own writes" true exists
  | None -> fail "transaction failed"

let test_edge_created_in_txn_visible () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let result =
    Gvecdb.with_transaction db (fun txn ->
        let a = ok_exn (Gvecdb.create_node db ~txn "person") in
        let b = ok_exn (Gvecdb.create_node db ~txn "person") in
        let e = ok_exn (Gvecdb.create_edge db ~txn "knows" a b) in
        let outbound = ok_exn (Gvecdb.get_outbound_edges db ~txn a) in
        (e, List.length outbound))
  in
  match result with
  | Some (_, count) -> check int "edge visible in txn" 1 count
  | None -> fail "transaction failed"

let test_props_visible_in_txn () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let result =
    Gvecdb.with_transaction db (fun txn ->
        let node = create_person db ~txn "Alice" 30 "alice@test.com" "bio" in
        let name = get_person_name db ~txn node in
        name)
  in
  match result with
  | Some name -> check string "props visible in txn" "Alice" name
  | None -> fail "transaction failed"

(** {1 Multiple operations in single transaction} *)

let test_multiple_nodes_in_txn () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let result =
    Gvecdb.with_transaction db (fun txn ->
        let nodes =
          List.init 10 (fun _ -> ok_exn (Gvecdb.create_node db ~txn "person"))
        in
        nodes)
  in
  match result with
  | Some nodes ->
      check int "10 nodes created" 10 (List.length nodes);
      List.iter
        (fun n ->
          check bool "node exists" true (ok_exn (Gvecdb.node_exists db n)))
        nodes
  | None -> fail "transaction failed"

let test_create_and_delete_in_txn () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let result =
    Gvecdb.with_transaction db (fun txn ->
        let node = ok_exn (Gvecdb.create_node db ~txn "person") in
        ok_exn (Gvecdb.delete_node db ~txn node);
        node)
  in
  match result with
  | Some node_id ->
      check bool "deleted node not exists" false
        (ok_exn (Gvecdb.node_exists db node_id))
  | None -> fail "transaction failed"

let test_complex_graph_in_txn () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let result =
    Gvecdb.with_transaction db (fun txn ->
        let a = ok_exn (Gvecdb.create_node db ~txn "person") in
        let b = ok_exn (Gvecdb.create_node db ~txn "person") in
        let c = ok_exn (Gvecdb.create_node db ~txn "person") in
        let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" a b) in
        let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" b c) in
        let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" a c) in
        let a_out = ok_exn (Gvecdb.get_outbound_edges db ~txn a) in
        let b_out = ok_exn (Gvecdb.get_outbound_edges db ~txn b) in
        let c_in = ok_exn (Gvecdb.get_inbound_edges db ~txn c) in
        (List.length a_out, List.length b_out, List.length c_in))
  in
  match result with
  | Some (a_out, b_out, c_in) ->
      check int "A has 2 outbound" 2 a_out;
      check int "B has 1 outbound" 1 b_out;
      check int "C has 2 inbound" 2 c_in
  | None -> fail "transaction failed"

(** {1 Read-only transaction tests} *)

let test_ro_transaction_sees_committed () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let alice = create_person db "Alice" 30 "alice@test.com" "bio" in
  let result =
    Gvecdb.with_transaction_ro db (fun txn ->
        let name = get_person_name db ~txn alice in
        name)
  in
  match result with
  | Some name -> check string "ro sees committed" "Alice" name
  | None -> fail "ro transaction failed"

let test_multiple_ro_transactions () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let node = ok_exn (Gvecdb.create_node db "person") in
  for _ = 1 to 5 do
    match
      Gvecdb.with_transaction_ro db (fun txn ->
          ok_exn (Gvecdb.node_exists db ~txn node))
    with
    | Some exists -> check bool "node visible in ro" true exists
    | None -> fail "ro transaction failed"
  done

(** {1 Nested operation tests} *)

let test_props_update_in_txn () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let node = create_person db "Alice" 30 "alice@test.com" "bio" in
  let result =
    Gvecdb.with_transaction db (fun txn ->
        ok_exn
          (Gvecdb.set_node_props_capnp db ~txn node "person"
             (fun b ->
               SchemaBuilder.Builder.Person.name_set b "Alice Updated";
               SchemaBuilder.Builder.Person.age_set_int_exn b 31;
               SchemaBuilder.Builder.Person.email_set b "alice@test.com";
               SchemaBuilder.Builder.Person.bio_set b "bio")
             SchemaBuilder.Builder.Person.init_root
             SchemaBuilder.Builder.Person.to_message);
        get_person_name db ~txn node)
  in
  match result with
  | Some name ->
      check string "updated in txn" "Alice Updated" name;
      check string "persisted after commit" "Alice Updated"
        (get_person_name db node)
  | None -> fail "transaction failed"

(** {1 Concurrent transaction tests using Domains} *)

let test_concurrent_ro_transactions () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let nodes =
    List.init 100 (fun i ->
        create_person db (Printf.sprintf "Person%d" i) i "email" "bio")
  in
  let num_domains = 4 in
  let iterations = 50 in
  let results = Array.make num_domains true in
  let domains =
    Array.init num_domains (fun i ->
        Domain.spawn (fun () ->
            for _ = 1 to iterations do
              match
                Gvecdb.with_transaction_ro db (fun txn ->
                    List.for_all
                      (fun node -> ok_exn (Gvecdb.node_exists db ~txn node))
                      nodes)
              with
              | Some all_exist -> if not all_exist then results.(i) <- false
              | None -> results.(i) <- false
            done))
  in
  Array.iter Domain.join domains;
  Array.iteri
    (fun i r -> check bool (Printf.sprintf "domain %d succeeded" i) true r)
    results

let test_concurrent_ro_sees_consistent_snapshot () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let alice = create_person db "Alice" 30 "alice@test.com" "bio" in
  let bob = create_person db "Bob" 25 "bob@test.com" "bio" in
  let _ = ok_exn (Gvecdb.create_edge db "knows" alice bob) in
  let ro_result = ref None in
  let ro_done = Atomic.make false in
  let writer_started = Atomic.make false in
  let ro_domain =
    Domain.spawn (fun () ->
        Gvecdb.with_transaction_ro db (fun txn ->
            Atomic.set writer_started true;
            Unix.sleepf 0.01;
            let name = get_person_name db ~txn alice in
            let edges = ok_exn (Gvecdb.get_outbound_edges db ~txn alice) in
            ro_result := Some (name, List.length edges);
            Atomic.set ro_done true;
            ()))
  in
  while not (Atomic.get writer_started) do
    Unix.sleepf 0.001
  done;
  let _ =
    Gvecdb.with_transaction db (fun txn ->
        ok_exn
          (Gvecdb.set_node_props_capnp db ~txn alice "person"
             (fun b ->
               SchemaBuilder.Builder.Person.name_set b "Alice Modified";
               SchemaBuilder.Builder.Person.age_set_int_exn b 31;
               SchemaBuilder.Builder.Person.email_set b "alice@test.com";
               SchemaBuilder.Builder.Person.bio_set b "bio")
             SchemaBuilder.Builder.Person.init_root
             SchemaBuilder.Builder.Person.to_message);
        let charlie = ok_exn (Gvecdb.create_node db ~txn "person") in
        let _ = ok_exn (Gvecdb.create_edge db ~txn "knows" alice charlie) in
        ())
  in
  let _ = Domain.join ro_domain in
  match !ro_result with
  | Some (name, edge_count) ->
      check string "RO sees original name" "Alice" name;
      check int "RO sees original edge count" 1 edge_count
  | None -> fail "RO transaction failed"

let test_concurrent_writes_serialized () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let counter = Atomic.make 0 in
  let num_domains = 4 in
  let iterations = 10 in
  let domains =
    Array.init num_domains (fun _ ->
        Domain.spawn (fun () ->
            for _ = 1 to iterations do
              match
                Gvecdb.with_transaction db (fun txn ->
                    let _ = ok_exn (Gvecdb.create_node db ~txn "person") in
                    Atomic.incr counter;
                    ())
              with
              | Some () -> ()
              | None -> ()
            done))
  in
  Array.iter Domain.join domains;
  check int "all writes succeeded" (num_domains * iterations)
    (Atomic.get counter)

let test_writer_blocks_writer () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let first_started = Atomic.make false in
  let first_done = Atomic.make false in
  let second_waited = Atomic.make false in
  let d1 =
    Domain.spawn (fun () ->
        Gvecdb.with_transaction db (fun txn ->
            let _ = ok_exn (Gvecdb.create_node db ~txn "person") in
            Atomic.set first_started true;
            Unix.sleepf 0.05;
            Atomic.set first_done true;
            ()))
  in
  while not (Atomic.get first_started) do
    Unix.sleepf 0.001
  done;
  let d2 =
    Domain.spawn (fun () ->
        Gvecdb.with_transaction db (fun txn ->
            if Atomic.get first_done then Atomic.set second_waited true;
            let _ = ok_exn (Gvecdb.create_node db ~txn "person") in
            ()))
  in
  let _ = Domain.join d1 in
  let _ = Domain.join d2 in
  check bool "second writer waited for first" true (Atomic.get second_waited)

let test_concurrent_reads_during_write () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let node = create_person db "Original" 1 "email" "bio" in
  let read_results = Array.make 10 "" in
  let writer_started = Atomic.make false in
  let writer_done = Atomic.make false in
  let writer =
    Domain.spawn (fun () ->
        Gvecdb.with_transaction db (fun txn ->
            ok_exn
              (Gvecdb.set_node_props_capnp db ~txn node "person"
                 (fun b ->
                   SchemaBuilder.Builder.Person.name_set b "Modified";
                   SchemaBuilder.Builder.Person.age_set_int_exn b 2;
                   SchemaBuilder.Builder.Person.email_set b "email";
                   SchemaBuilder.Builder.Person.bio_set b "bio")
                 SchemaBuilder.Builder.Person.init_root
                 SchemaBuilder.Builder.Person.to_message);
            Atomic.set writer_started true;
            Unix.sleepf 0.03;
            Atomic.set writer_done true;
            ()))
  in
  while not (Atomic.get writer_started) do
    Unix.sleepf 0.001
  done;
  let readers =
    Array.init 10 (fun i ->
        Domain.spawn (fun () ->
            match
              Gvecdb.with_transaction_ro db (fun txn ->
                  get_person_name db ~txn node)
            with
            | Some name -> read_results.(i) <- name
            | None -> read_results.(i) <- "FAILED"))
  in
  Array.iter Domain.join readers;
  let _ = Domain.join writer in
  Array.iteri
    (fun i result ->
      check string
        (Printf.sprintf "reader %d sees original" i)
        "Original" result)
    read_results;
  check string "after commit sees modified" "Modified" (get_person_name db node)

let test_high_contention_scenario () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let num_domains = 8 in
  let ops_per_domain = 20 in
  let success_count = Atomic.make 0 in
  let domains =
    Array.init num_domains (fun i ->
        Domain.spawn (fun () ->
            for j = 1 to ops_per_domain do
              if j mod 3 = 0 then begin
                match
                  Gvecdb.with_transaction db (fun txn ->
                      let n = ok_exn (Gvecdb.create_node db ~txn "person") in
                      ok_exn
                        (Gvecdb.set_node_props_capnp db ~txn n "person"
                           (fun b ->
                             SchemaBuilder.Builder.Person.name_set b
                               (Printf.sprintf "Person_%d_%d" i j);
                             SchemaBuilder.Builder.Person.age_set_int_exn b j;
                             SchemaBuilder.Builder.Person.email_set b "email";
                             SchemaBuilder.Builder.Person.bio_set b "bio")
                           SchemaBuilder.Builder.Person.init_root
                           SchemaBuilder.Builder.Person.to_message);
                      n)
                with
                | Some _ -> Atomic.incr success_count
                | None -> ()
              end
              else begin
                match
                  Gvecdb.with_transaction_ro db (fun txn ->
                      let _ = ok_exn (Gvecdb.node_exists db ~txn 0L) in
                      ())
                with
                | Some () -> Atomic.incr success_count
                | None -> ()
              end
            done))
  in
  Array.iter Domain.join domains;
  let total_ops = num_domains * ops_per_domain in
  let successes = Atomic.get success_count in
  check bool "most operations succeeded" true (successes > total_ops / 2);
  Printf.printf "High contention: %d/%d operations succeeded\n%!" successes
    total_ops

(** {1 Test runner} *)

let commit_tests =
  [
    ("transaction_commits", `Quick, test_transaction_commits);
    ("rollback_on_exception", `Quick, test_transaction_rollback_on_exception);
    ("abort", `Quick, test_transaction_abort);
    ("abort_ro", `Quick, test_transaction_abort_ro);
  ]

let edge_rollback_tests =
  [
    ("edge_creation_rollback", `Quick, test_edge_creation_rollback);
    ("multiple_edges_rollback", `Quick, test_multiple_edges_rollback);
    ("edge_deletion_rollback", `Quick, test_edge_deletion_rollback);
  ]

let node_rollback_tests =
  [
    ("node_deletion_rollback", `Quick, test_node_deletion_rollback);
    ( "node_with_props_deletion_rollback",
      `Quick,
      test_node_with_props_deletion_rollback );
  ]

let props_rollback_tests =
  [
    ("props_creation_rollback", `Quick, test_props_creation_rollback);
    ("props_update_rollback", `Quick, test_props_update_rollback);
    ("edge_props_rollback", `Quick, test_edge_props_rollback);
  ]

let complex_rollback_tests =
  [
    ("complex_graph_rollback", `Quick, test_complex_graph_rollback);
    ("partial_operations_rollback", `Quick, test_partial_operations_rollback);
  ]

let isolation_tests =
  [
    ("read_own_writes", `Quick, test_read_own_writes);
    ("edge_visible_in_txn", `Quick, test_edge_created_in_txn_visible);
    ("props_visible_in_txn", `Quick, test_props_visible_in_txn);
  ]

let multi_op_tests =
  [
    ("multiple_nodes", `Quick, test_multiple_nodes_in_txn);
    ("create_and_delete", `Quick, test_create_and_delete_in_txn);
    ("complex_graph", `Quick, test_complex_graph_in_txn);
  ]

let ro_tests =
  [
    ("ro_sees_committed", `Quick, test_ro_transaction_sees_committed);
    ("multiple_ro", `Quick, test_multiple_ro_transactions);
  ]

let nested_tests = [ ("props_update_in_txn", `Quick, test_props_update_in_txn) ]

let concurrency_tests =
  [
    ("concurrent_ro_transactions", `Slow, test_concurrent_ro_transactions);
    ( "concurrent_ro_sees_consistent_snapshot",
      `Slow,
      test_concurrent_ro_sees_consistent_snapshot );
    ("concurrent_writes_serialized", `Slow, test_concurrent_writes_serialized);
    ("writer_blocks_writer", `Slow, test_writer_blocks_writer);
    ("concurrent_reads_during_write", `Slow, test_concurrent_reads_during_write);
    ("high_contention_scenario", `Slow, test_high_contention_scenario);
  ]

let () =
  run "Transactions"
    [
      ("commit_rollback", commit_tests);
      ("edge_rollback", edge_rollback_tests);
      ("node_rollback", node_rollback_tests);
      ("props_rollback", props_rollback_tests);
      ("complex_rollback", complex_rollback_tests);
      ("isolation", isolation_tests);
      ("multi_ops", multi_op_tests);
      ("read_only", ro_tests);
      ("nested", nested_tests);
      ("concurrency", concurrency_tests);
    ]
