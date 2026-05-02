open Alcotest
open Test_common
module Bigstring = Bigstringaf

let floats_to_bigstring (arr : float array) : Gvecdb.bigstring =
  let n = Array.length arr in
  let bs = Bigstring.create (n * 4) in
  for i = 0 to n - 1 do
    Bigstring.set_int32_le bs (i * 4) (Int32.bits_of_float arr.(i))
  done;
  bs

let with_txn db f =
  match Gvecdb.with_transaction db f with
  | Some x -> x
  | None -> Alcotest.fail "transaction aborted unexpectedly"

let test_vector_crud () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  let original = [| 1.5; 2.5; 3.5; 4.5 |] in
  let data = floats_to_bigstring original in
  let vec_id =
    with_txn db (fun txn ->
        ok_exn (Gvecdb.create_vector db ~txn Node node "embedding" data))
  in
  check bool "exists" true (ok_exn (Gvecdb.vector_exists db vec_id));
  let info = ok_exn (Gvecdb.get_vector_info db vec_id) in
  check bool "owner is Node" true (info.owner_kind = Gvecdb.Node);
  check int64 "correct owner" node info.owner_id;
  check string "correct tag" "embedding" info.vector_tag;
  let retrieved = ok_exn (Gvecdb.get_vector db vec_id) in
  check int "same length" (Bigstring.length data) (Bigstring.length retrieved);
  let norm_sq = Array.fold_left (fun acc x -> acc +. (x *. x)) 0.0 original in
  let norm = sqrt norm_sq in
  for i = 0 to 3 do
    let expected = original.(i) /. norm in
    let actual =
      Int32.float_of_bits (Bigstring.get_int32_le retrieved (i * 4))
    in
    check bool
      (Printf.sprintf "normalized value %d" i)
      true
      (Float.abs (expected -. actual) < 1e-6)
  done;
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn vec_id));
  check bool "gone after delete" false (ok_exn (Gvecdb.vector_exists db vec_id));
  match Gvecdb.get_vector db vec_id with
  | Error (Gvecdb.Vector_not_found _) -> ()
  | _ -> fail "expected Vector_not_found"

let test_vector_unnormalized () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  let original = [| 1.5; 2.5; 3.5; 4.5 |] in
  let data = floats_to_bigstring original in
  let vec_id =
    with_txn db (fun txn ->
        ok_exn
          (Gvecdb.create_vector db ~txn ~normalize:false Node node "embedding"
             data))
  in
  let retrieved = ok_exn (Gvecdb.get_vector db vec_id) in
  for i = 0 to 3 do
    check int32
      (Printf.sprintf "raw value %d" i)
      (Int32.bits_of_float original.(i))
      (Bigstring.get_int32_le retrieved (i * 4))
  done

let test_multi_vector_and_tags () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  let v1, v2, v3 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node node "title"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node node "content"
               (floats_to_bigstring [| 0.0; 1.0 |]))
        in
        let v3 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node node "title"
               (floats_to_bigstring [| 0.5; 0.5 |]))
        in
        (v1, v2, v3))
  in
  let all = ok_exn (Gvecdb.get_vectors db Node node ()) in
  check int "three total" 3 (List.length all);
  let content =
    ok_exn (Gvecdb.get_vectors db Node node ~vector_tag:"content" ())
  in
  check int "one content" 1 (List.length content);
  check int64 "correct content id" v2 (List.hd content).vector_id;
  let title = ok_exn (Gvecdb.get_vectors db Node node ~vector_tag:"title" ()) in
  check int "two title" 2 (List.length title);
  let empty =
    ok_exn (Gvecdb.get_vectors db Node node ~vector_tag:"nonexistent" ())
  in
  check int "zero nonexistent" 0 (List.length empty);
  ignore (v1, v3)

let test_error_paths () =
  with_temp_db "vectors" @@ fun db ->
  check bool "nonexistent" false (ok_exn (Gvecdb.vector_exists db 999999L));
  (match Gvecdb.get_vector db 999999L with
  | Error (Gvecdb.Vector_not_found _) -> ()
  | _ -> fail "expected Vector_not_found");
  with_txn db (fun txn ->
      (match
         Gvecdb.create_vector db ~txn Node 999999L "e"
           (floats_to_bigstring [| 1.0 |])
       with
      | Error (Gvecdb.Node_not_found _) -> ()
      | _ -> fail "expected Node_not_found");
      match
        Gvecdb.create_vector db ~txn Edge 999999L "e"
          (floats_to_bigstring [| 1.0 |])
      with
      | Error (Gvecdb.Edge_not_found _) -> ()
      | _ -> fail "expected Edge_not_found")

let test_knn_euclidean () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let n3 = ok_exn (Gvecdb.create_node db "doc") in
  let v2 =
    with_txn db (fun txn ->
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n1 "e"
               (floats_to_bigstring [| 0.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n2 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n3 "e"
               (floats_to_bigstring [| 10.0; 10.0 |]))
        in
        v2)
  in
  let results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Euclidean ~k:2 [| 0.9; 0.1 |])
  in
  check int "two results" 2 (List.length results);
  check int64 "nearest is v2" v2 (List.hd results).Gvecdb.vector_id;
  check bool "ascending" true
    ((List.nth results 0).distance < (List.nth results 1).distance)

let test_knn_cosine () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n1 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n2 "e"
               (floats_to_bigstring [| 0.0; 1.0 |]))
        in
        v1)
  in
  let results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Cosine ~k:1 [| 10.0; 0.0 |])
  in
  check int64 "nearest is v1" v1 (List.hd results).Gvecdb.vector_id;
  check (float 0.0001) "zero cosine distance" 0.0 (List.hd results).distance

let test_knn_dot_product () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v1 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n1 "e"
               (floats_to_bigstring [| 2.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n2 "e"
               (floats_to_bigstring [| 1.0; 1.0 |]))
        in
        v1)
  in
  let results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:DotProduct ~k:1 [| 1.0; 0.0 |])
  in
  check int64 "highest dot product" v1 (List.hd results).Gvecdb.vector_id

let test_knn_edge_cases () =
  with_temp_db "vectors" @@ fun db ->
  let results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Euclidean ~k:10 [| 1.0; 0.0 |])
  in
  check int "empty db" 0 (List.length results);
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let v2 =
    with_txn db (fun txn ->
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n1 "e"
               (floats_to_bigstring [| 0.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n2 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        v2)
  in
  let results2 =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Cosine ~k:2 [| 1.0; 0.0 |])
  in
  check int64 "nearest non-zero" v2 (List.hd results2).Gvecdb.vector_id;
  let results3 =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Euclidean ~k:100 [| 1.0; 0.0 |])
  in
  check int "k > n returns n" 2 (List.length results3)

let test_dimension_mismatch () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  with_txn db (fun txn ->
      let _ =
        ok_exn
          (Gvecdb.create_vector db ~txn Node n1 "e"
             (floats_to_bigstring [| 1.0; 0.0 |]))
      in
      ());
  let result =
    with_txn db (fun txn ->
        Gvecdb.create_vector db ~txn Node n2 "e"
          (floats_to_bigstring [| 1.0; 0.0; 0.0 |]))
  in
  match result with
  | Error (Gvecdb.Corrupted_data _) -> ()
  | _ -> fail "expected dimension mismatch error"

let test_mixed_node_edge_vectors () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let edge = ok_exn (Gvecdb.create_edge db "related" n1 n2) in
  let v_edge =
    with_txn db (fun txn ->
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n1 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let v_edge =
          ok_exn
            (Gvecdb.create_vector db ~txn Edge edge "e"
               (floats_to_bigstring [| 0.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn Node n2 "e"
               (floats_to_bigstring [| 10.0; 10.0 |]))
        in
        v_edge)
  in
  let results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Euclidean ~k:3 [| 0.1; 0.0 |])
  in
  check int "three results" 3 (List.length results);
  check int64 "nearest is edge vector" v_edge (List.hd results).vector_id;
  check bool "has node results" true
    (List.exists (fun r -> r.Gvecdb.owner_kind = Gvecdb.Node) results);
  check bool "has edge results" true
    (List.exists (fun r -> r.Gvecdb.owner_kind = Gvecdb.Edge) results)

let () =
  run "Vectors"
    [
      ( "crud",
        [
          ("vector_crud", `Quick, test_vector_crud);
          ("vector_unnormalized", `Quick, test_vector_unnormalized);
          ("multi_vector_and_tags", `Quick, test_multi_vector_and_tags);
          ("error_paths", `Quick, test_error_paths);
          ("dimension_mismatch", `Quick, test_dimension_mismatch);
        ] );
      ( "knn",
        [
          ("knn_euclidean", `Quick, test_knn_euclidean);
          ("knn_cosine", `Quick, test_knn_cosine);
          ("knn_dot_product", `Quick, test_knn_dot_product);
          ("knn_edge_cases", `Quick, test_knn_edge_cases);
          ("mixed_node_edge_vectors", `Quick, test_mixed_node_edge_vectors);
        ] );
    ]
