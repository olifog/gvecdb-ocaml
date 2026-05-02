open Alcotest
open Test_common

let test_commit_and_rollback () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let result =
    Gvecdb.with_transaction db (fun txn ->
        ok_exn (Gvecdb.create_node db ~txn "person"))
  in
  (match result with
  | Some node_id ->
      check bool "node exists after commit" true
        (ok_exn (Gvecdb.node_exists db node_id))
  | None -> fail "transaction should have committed");
  let node_before = ok_exn (Gvecdb.create_node db "person") in
  let next = Int64.add node_before 1L in
  (try
     let _ =
       Gvecdb.with_transaction db (fun txn ->
           let _ = ok_exn (Gvecdb.create_node db ~txn "person") in
           failwith "simulated error")
     in
     ()
   with Failure _ -> ());
  check bool "rolled back" false (ok_exn (Gvecdb.node_exists db next));
  let abort_result =
    Gvecdb.with_transaction db (fun txn ->
        let _ = ok_exn (Gvecdb.create_node db ~txn "person") in
        Gvecdb.abort_transaction txn)
  in
  check (option reject) "abort returns None" None abort_result

let test_complex_rollback () =
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
           let builder = SchemaBuilder.Builder.Person.init_root () in
           SchemaBuilder.Builder.Person.name_set builder "Alice Modified";
           SchemaBuilder.Builder.Person.age_set_int_exn builder 31;
           SchemaBuilder.Builder.Person.email_set builder "alice2@test.com";
           SchemaBuilder.Builder.Person.bio_set builder "new bio";
           let bs =
             capnp_to_bigstring SchemaBuilder.Builder.Person.to_message builder
           in
           ok_exn (Gvecdb.set_node_props db ~txn alice "person" bs);
           failwith "abort")
     in
     ()
   with Failure _ -> ());
  check string "alice name unchanged" "Alice" (get_person_name db alice);
  check int "alice outbound unchanged" 1
    (List.length (ok_exn (Gvecdb.get_outbound_edges db alice ())));
  check int "bob outbound unchanged" 0
    (List.length (ok_exn (Gvecdb.get_outbound_edges db bob ())))

let test_read_own_writes () =
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
        let node = create_person db ~txn "Alice" 30 "alice@test.com" "bio" in
        let name = get_person_name db ~txn node in
        let a_out = ok_exn (Gvecdb.get_outbound_edges db ~txn a ()) in
        let c_in = ok_exn (Gvecdb.get_inbound_edges db ~txn c ()) in
        (name, List.length a_out, List.length c_in))
  in
  match result with
  | Some (name, a_out, c_in) ->
      check string "props visible" "Alice" name;
      check int "a has 2 outbound" 2 a_out;
      check int "c has 2 inbound" 2 c_in
  | None -> fail "transaction failed"

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
  let writer_started = Atomic.make false in
  let ro_domain =
    Domain.spawn (fun () ->
        Gvecdb.with_transaction_ro db (fun txn ->
            Atomic.set writer_started true;
            Unix.sleepf 0.01;
            let name = get_person_name db ~txn alice in
            let edges = ok_exn (Gvecdb.get_outbound_edges db ~txn alice ()) in
            ro_result := Some (name, List.length edges);
            ()))
  in
  while not (Atomic.get writer_started) do
    Unix.sleepf 0.001
  done;
  let _ =
    Gvecdb.with_transaction db (fun txn ->
        let builder = SchemaBuilder.Builder.Person.init_root () in
        SchemaBuilder.Builder.Person.name_set builder "Alice Modified";
        SchemaBuilder.Builder.Person.age_set_int_exn builder 31;
        SchemaBuilder.Builder.Person.email_set builder "alice@test.com";
        SchemaBuilder.Builder.Person.bio_set builder "bio";
        let bs =
          capnp_to_bigstring SchemaBuilder.Builder.Person.to_message builder
        in
        ok_exn (Gvecdb.set_node_props db ~txn alice "person" bs);
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
            let builder = SchemaBuilder.Builder.Person.init_root () in
            SchemaBuilder.Builder.Person.name_set builder "Modified";
            SchemaBuilder.Builder.Person.age_set_int_exn builder 2;
            SchemaBuilder.Builder.Person.email_set builder "email";
            SchemaBuilder.Builder.Person.bio_set builder "bio";
            let bs =
              capnp_to_bigstring SchemaBuilder.Builder.Person.to_message builder
            in
            ok_exn (Gvecdb.set_node_props db ~txn node "person" bs);
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

let test_high_contention () =
  with_temp_db "txn" @@ fun db ->
  register_schemas db;
  let num_domains = 8 in
  let ops_per_domain = 20 in
  let success_count = Atomic.make 0 in
  let domains =
    Array.init num_domains (fun i ->
        Domain.spawn (fun () ->
            for j = 1 to ops_per_domain do
              if j mod 3 = 0 then
                match
                  Gvecdb.with_transaction db (fun txn ->
                      let n = ok_exn (Gvecdb.create_node db ~txn "person") in
                      let builder = SchemaBuilder.Builder.Person.init_root () in
                      SchemaBuilder.Builder.Person.name_set builder
                        (Printf.sprintf "Person_%d_%d" i j);
                      SchemaBuilder.Builder.Person.age_set_int_exn builder j;
                      SchemaBuilder.Builder.Person.email_set builder "email";
                      SchemaBuilder.Builder.Person.bio_set builder "bio";
                      let bs =
                        capnp_to_bigstring
                          SchemaBuilder.Builder.Person.to_message builder
                      in
                      ok_exn (Gvecdb.set_node_props db ~txn n "person" bs);
                      n)
                with
                | Some _ -> Atomic.incr success_count
                | None -> ()
              else
                match
                  Gvecdb.with_transaction_ro db (fun txn ->
                      let _ = ok_exn (Gvecdb.node_exists db ~txn 0L) in
                      ())
                with
                | Some () -> Atomic.incr success_count
                | None -> ()
            done))
  in
  Array.iter Domain.join domains;
  let total_ops = num_domains * ops_per_domain in
  check bool "most operations succeeded" true
    (Atomic.get success_count > total_ops / 2)

let () =
  run "Transactions"
    [
      ( "semantics",
        [
          ("commit_and_rollback", `Quick, test_commit_and_rollback);
          ("complex_rollback", `Quick, test_complex_rollback);
          ("read_own_writes", `Quick, test_read_own_writes);
        ] );
      ( "concurrency",
        [
          ("concurrent_ro", `Slow, test_concurrent_ro_transactions);
          ( "ro_consistent_snapshot",
            `Slow,
            test_concurrent_ro_sees_consistent_snapshot );
          ("writes_serialized", `Slow, test_concurrent_writes_serialized);
          ("writer_blocks_writer", `Slow, test_writer_blocks_writer);
          ("reads_during_write", `Slow, test_concurrent_reads_during_write);
          ("high_contention", `Slow, test_high_contention);
        ] );
    ]
