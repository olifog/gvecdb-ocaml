(** Vector storage and k-NN tests *)

open Alcotest
open Test_common
module Bigstring = Bigstringaf

(** convert float array to bigstring (float32 little-endian) *)
let floats_to_bigstring (arr : float array) : Gvecdb.bigstring =
  let n = Array.length arr in
  let bs = Bigstring.create (n * 4) in
  for i = 0 to n - 1 do
    Bigstring.set_int32_le bs (i * 4) (Int32.bits_of_float arr.(i))
  done;
  bs

(** Helper to run vector operations in a transaction and unwrap result *)
let with_txn db f =
  match Gvecdb.with_transaction db f with
  | Some x -> x
  | None -> Alcotest.fail "transaction aborted unexpectedly"

(** {1 vector CRUD tests} *)

let test_create_vector () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  let data = floats_to_bigstring [| 1.0; 2.0; 3.0 |] in
  let vec_id =
    with_txn db (fun txn ->
        ok_exn (Gvecdb.create_vector db ~txn node "embedding" data))
  in
  check bool "vector exists after creation" true
    (ok_exn (Gvecdb.vector_exists db vec_id))

let test_get_vector () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  let original = [| 1.5; 2.5; 3.5; 4.5 |] in
  let data = floats_to_bigstring original in
  let vec_id =
    with_txn db (fun txn ->
        ok_exn (Gvecdb.create_vector db ~txn node "embedding" data))
  in
  let retrieved = ok_exn (Gvecdb.get_vector db vec_id) in
  (* Vectors are stored normalized, so length should be same but values differ *)
  check int "same length" (Bigstring.length data) (Bigstring.length retrieved);
  (* Compute expected normalized values *)
  let norm_sq = Array.fold_left (fun acc x -> acc +. (x *. x)) 0.0 original in
  let norm = sqrt norm_sq in
  for i = 0 to 3 do
    let expected = Int32.bits_of_float (original.(i) /. norm) in
    let actual = Bigstring.get_int32_le retrieved (i * 4) in
    check int32 (Printf.sprintf "normalized value %d" i) expected actual
  done

let test_get_vector_unnormalized () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  let original = [| 1.5; 2.5; 3.5; 4.5 |] in
  let data = floats_to_bigstring original in
  let vec_id =
    with_txn db (fun txn ->
        ok_exn
          (Gvecdb.create_vector db ~txn ~normalize:false node "embedding" data))
  in
  let retrieved = ok_exn (Gvecdb.get_vector db vec_id) in
  check int "same length" (Bigstring.length data) (Bigstring.length retrieved);
  (* With normalize:false, values should be stored as-is *)
  for i = 0 to 3 do
    let expected = Int32.bits_of_float original.(i) in
    let actual = Bigstring.get_int32_le retrieved (i * 4) in
    check int32 (Printf.sprintf "raw value %d" i) expected actual
  done

let test_get_vector_info () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  let data = floats_to_bigstring [| 1.0; 2.0; 3.0 |] in
  let vec_id =
    with_txn db (fun txn ->
        ok_exn (Gvecdb.create_vector db ~txn node "content_embedding" data))
  in
  let info = ok_exn (Gvecdb.get_vector_info db vec_id) in
  check int64 "correct vector_id" vec_id info.vector_id;
  check bool "owner is Node" true (info.owner_kind = Gvecdb.Node);
  check int64 "correct owner_id" node info.owner_id;
  check string "correct tag" "content_embedding" info.vector_tag

let test_delete_vector () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  let data = floats_to_bigstring [| 1.0; 2.0; 3.0 |] in
  let vec_id =
    with_txn db (fun txn ->
        ok_exn (Gvecdb.create_vector db ~txn node "embedding" data))
  in
  check bool "exists before delete" true
    (ok_exn (Gvecdb.vector_exists db vec_id));
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn vec_id));
  check bool "not exists after delete" false
    (ok_exn (Gvecdb.vector_exists db vec_id));
  match Gvecdb.get_vector db vec_id with
  | Error (Gvecdb.Vector_not_found _) -> ()
  | _ -> fail "expected Vector_not_found error"

let test_vector_not_found () =
  with_temp_db "vectors" @@ fun db ->
  check bool "nonexistent vector" false
    (ok_exn (Gvecdb.vector_exists db 999999L));
  match Gvecdb.get_vector db 999999L with
  | Error (Gvecdb.Vector_not_found _) -> ()
  | _ -> fail "expected Vector_not_found error"

let test_create_vector_node_not_found () =
  with_temp_db "vectors" @@ fun db ->
  let data = floats_to_bigstring [| 1.0; 2.0; 3.0 |] in
  with_txn db (fun txn ->
      match Gvecdb.create_vector db ~txn 999999L "embedding" data with
      | Error (Gvecdb.Node_not_found _) -> ()
      | _ -> fail "expected Node_not_found error")

(** {1 multiple vectors tests} *)

let test_multiple_vectors_per_node () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  let v1, v2, v3 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn node "title_embedding"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn node "content_embedding"
               (floats_to_bigstring [| 0.0; 1.0 |]))
        in
        let v3 =
          ok_exn
            (Gvecdb.create_vector db ~txn node "summary_embedding"
               (floats_to_bigstring [| 0.5; 0.5 |]))
        in
        (v1, v2, v3))
  in
  let all_vecs = ok_exn (Gvecdb.get_vectors_for_node db node ()) in
  check int "three vectors" 3 (List.length all_vecs);
  let ids = List.map (fun (v : Gvecdb.vector_info) -> v.vector_id) all_vecs in
  check bool "v1 in list" true (List.mem v1 ids);
  check bool "v2 in list" true (List.mem v2 ids);
  check bool "v3 in list" true (List.mem v3 ids)

let test_get_vectors_by_tag () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  let v2 =
    with_txn db (fun txn ->
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn node "title"
               (floats_to_bigstring [| 1.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn node "content"
               (floats_to_bigstring [| 2.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn node "title"
               (floats_to_bigstring [| 3.0 |]))
        in
        v2)
  in
  let content_vecs =
    ok_exn (Gvecdb.get_vectors_for_node db node ~vector_tag:"content" ())
  in
  check int "one content vector" 1 (List.length content_vecs);
  let first_vec : Gvecdb.vector_info = List.hd content_vecs in
  check int64 "correct id" v2 first_vec.vector_id

let test_get_vectors_nonexistent_tag () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  with_txn db (fun txn ->
      let _ =
        ok_exn
          (Gvecdb.create_vector db ~txn node "embedding"
             (floats_to_bigstring [| 1.0 |]))
      in
      ());
  let vecs =
    ok_exn (Gvecdb.get_vectors_for_node db node ~vector_tag:"nonexistent" ())
  in
  check int "no vectors" 0 (List.length vecs)

let test_vector_ids_sequential () =
  with_temp_db "vectors" @@ fun db ->
  let node = ok_exn (Gvecdb.create_node db "document") in
  let v1, v2, v3 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn node "e"
               (floats_to_bigstring [| 1.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn node "e"
               (floats_to_bigstring [| 2.0 |]))
        in
        let v3 =
          ok_exn
            (Gvecdb.create_vector db ~txn node "e"
               (floats_to_bigstring [| 3.0 |]))
        in
        (v1, v2, v3))
  in
  check int64 "v2 = v1 + 1" (Int64.add v1 1L) v2;
  check int64 "v3 = v2 + 1" (Int64.add v2 1L) v3

(** {1 k-NN tests} *)

let test_knn_euclidean () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let n3 = ok_exn (Gvecdb.create_node db "doc") in
  (* vectors: (0,0), (1,0), (10,10) *)
  let v2 =
    with_txn db (fun txn ->
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 0.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n3 "e"
               (floats_to_bigstring [| 10.0; 10.0 |]))
        in
        v2)
  in
  (* query (0.9, 0.1) - closest to (1,0), then (0,0), then (10,10) *)
  let query = [| 0.9; 0.1 |] in
  let results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Euclidean ~k:2 query)
  in
  check int "two results" 2 (List.length results);
  check int64 "nearest is v2" v2 (List.hd results).Gvecdb.vector_id;
  (* check distances are ascending *)
  let d1 = (List.nth results 0).Gvecdb.distance in
  let d2 = (List.nth results 1).Gvecdb.distance in
  check bool "d1 < d2" true (d1 < d2)

let test_knn_cosine () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  (* vectors with different magnitudes but same direction *)
  let v1 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 0.0; 1.0 |]))
        in
        v1)
  in
  (* query pointing in direction of v1 *)
  let query = [| 10.0; 0.0 |] in
  let results = ok_exn (Gvecdb.knn_brute_force db ~metric:Cosine ~k:1 query) in
  check int "one result" 1 (List.length results);
  check int64 "nearest is v1" v1 (List.hd results).Gvecdb.vector_id;
  (* cosine distance should be 0 (identical direction) *)
  check (float 0.0001) "zero cosine distance" 0.0
    (List.hd results).Gvecdb.distance

let test_knn_dot_product () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  (* v1 = (2,0), v2 = (1,1) *)
  let v1 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 2.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 1.0; 1.0 |]))
        in
        v1)
  in
  (* query = (1,0) -> dot with v1 = 2, dot with v2 = 1 *)
  let query = [| 1.0; 0.0 |] in
  let results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:DotProduct ~k:1 query)
  in
  check int "one result" 1 (List.length results);
  check int64 "nearest is v1 (higher dot product)" v1
    (List.hd results).Gvecdb.vector_id

let test_knn_empty_db () =
  with_temp_db "vectors" @@ fun db ->
  let query = [| 1.0; 0.0 |] in
  let results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Euclidean ~k:10 query)
  in
  check int "no results" 0 (List.length results)

let test_knn_cosine_zero_vector () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  (* v1 is a zero vector, v2 is a normal vector *)
  let v2 =
    with_txn db (fun txn ->
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 0.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        v2)
  in
  (* query with normal vector - should handle zero vector gracefully *)
  let query = [| 1.0; 0.0 |] in
  let results = ok_exn (Gvecdb.knn_brute_force db ~metric:Cosine ~k:2 query) in
  check int "two results" 2 (List.length results);
  (* v2 should be nearest (distance 0), zero vector should have max distance (1.0) *)
  check int64 "nearest is v2" v2 (List.hd results).Gvecdb.vector_id;
  check (float 0.0001) "v2 distance is 0" 0.0 (List.hd results).Gvecdb.distance;
  check (float 0.0001) "zero vec distance is 1" 1.0
    (List.nth results 1).Gvecdb.distance

let test_knn_dimension_mismatch () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  (* v1 has 2 dims, v2 has 3 dims *)
  let v1 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 1.0; 0.0; 0.0 |]))
        in
        v1)
  in
  (* query with 2 dims - should only match v1, v2 has wrong dimension *)
  let query = [| 1.0; 0.0 |] in
  let results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Euclidean ~k:10 query)
  in
  (* v2 should be filtered out due to dimension mismatch (infinity distance) *)
  check int "one matching result" 1 (List.length results);
  check int64 "only v1 matches" v1 (List.hd results).Gvecdb.vector_id

let test_knn_k_larger_than_db () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  with_txn db (fun txn ->
      let _ =
        ok_exn
          (Gvecdb.create_vector db ~txn n1 "e" (floats_to_bigstring [| 1.0 |]))
      in
      let _ =
        ok_exn
          (Gvecdb.create_vector db ~txn n1 "e" (floats_to_bigstring [| 2.0 |]))
      in
      ());
  let query = [| 1.5 |] in
  let results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Euclidean ~k:100 query)
  in
  check int "only 2 results" 2 (List.length results)

let test_knn_brute_force_bs () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let v1 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 1.0; 2.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 10.0; 20.0 |]))
        in
        v1)
  in
  let query_bs = floats_to_bigstring [| 1.1; 2.1 |] in
  let results =
    ok_exn (Gvecdb.knn_brute_force_bs db ~metric:Euclidean ~k:1 query_bs)
  in
  check int "one result" 1 (List.length results);
  check int64 "nearest is v1" v1 (List.hd results).Gvecdb.vector_id

let test_knn_mixed_normalized () =
  with_temp_db "vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  (* v1 stored normalized (default), v2 stored raw *)
  let v1, v2 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 3.0; 0.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_vector db ~txn ~normalize:false n2 "e"
               (floats_to_bigstring [| 0.0; 4.0 |]))
        in
        (v1, v2))
  in
  (* query = (1,0) should be closest to v1 for cosine *)
  let query = [| 1.0; 0.0 |] in
  let results = ok_exn (Gvecdb.knn_brute_force db ~metric:Cosine ~k:2 query) in
  check int "two results" 2 (List.length results);
  check int64 "nearest is v1 (same direction)" v1
    (List.hd results).Gvecdb.vector_id;
  check int64 "second is v2" v2 (List.nth results 1).Gvecdb.vector_id;
  (* v1 should have distance ~0, v2 should have distance ~1 *)
  check (float 0.0001) "v1 cosine distance ~0" 0.0
    (List.hd results).Gvecdb.distance;
  check (float 0.0001) "v2 cosine distance ~1" 1.0
    (List.nth results 1).Gvecdb.distance

(** {1 edge vector tests} *)

let test_create_edge_vector () =
  with_temp_db "edge_vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "person") in
  let n2 = ok_exn (Gvecdb.create_node db "person") in
  let edge = ok_exn (Gvecdb.create_edge db "knows" n1 n2) in
  let data = floats_to_bigstring [| 1.0; 2.0; 3.0 |] in
  let vec_id =
    with_txn db (fun txn ->
        ok_exn
          (Gvecdb.create_edge_vector db ~txn edge "relation_embedding" data))
  in
  check bool "edge vector exists after creation" true
    (ok_exn (Gvecdb.vector_exists db vec_id))

let test_get_edge_vector_info () =
  with_temp_db "edge_vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "person") in
  let n2 = ok_exn (Gvecdb.create_node db "person") in
  let edge = ok_exn (Gvecdb.create_edge db "knows" n1 n2) in
  let data = floats_to_bigstring [| 1.0; 2.0; 3.0 |] in
  let vec_id =
    with_txn db (fun txn ->
        ok_exn
          (Gvecdb.create_edge_vector db ~txn edge "relation_embedding" data))
  in
  let info = ok_exn (Gvecdb.get_vector_info db vec_id) in
  check int64 "correct vector_id" vec_id info.vector_id;
  check bool "owner is Edge" true (info.owner_kind = Gvecdb.Edge);
  check int64 "correct owner_id (edge)" edge info.owner_id;
  check string "correct tag" "relation_embedding" info.vector_tag

let test_get_edge_vector_data () =
  with_temp_db "edge_vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "person") in
  let n2 = ok_exn (Gvecdb.create_node db "person") in
  let edge = ok_exn (Gvecdb.create_edge db "knows" n1 n2) in
  let original = [| 1.5; 2.5; 3.5; 4.5 |] in
  let data = floats_to_bigstring original in
  let vec_id =
    with_txn db (fun txn ->
        ok_exn (Gvecdb.create_edge_vector db ~txn edge "embedding" data))
  in
  let retrieved = ok_exn (Gvecdb.get_vector db vec_id) in
  check int "same length" (Bigstring.length data) (Bigstring.length retrieved);
  (* Vectors are stored normalized *)
  let norm_sq = Array.fold_left (fun acc x -> acc +. (x *. x)) 0.0 original in
  let norm = sqrt norm_sq in
  for i = 0 to 3 do
    let expected = Int32.bits_of_float (original.(i) /. norm) in
    let actual = Bigstring.get_int32_le retrieved (i * 4) in
    check int32 (Printf.sprintf "normalized value %d" i) expected actual
  done

let test_create_edge_vector_edge_not_found () =
  with_temp_db "edge_vectors" @@ fun db ->
  let data = floats_to_bigstring [| 1.0; 2.0; 3.0 |] in
  with_txn db (fun txn ->
      match Gvecdb.create_edge_vector db ~txn 999999L "embedding" data with
      | Error (Gvecdb.Edge_not_found _) -> ()
      | _ -> fail "expected Edge_not_found error")

let test_delete_edge_vector () =
  with_temp_db "edge_vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "person") in
  let n2 = ok_exn (Gvecdb.create_node db "person") in
  let edge = ok_exn (Gvecdb.create_edge db "knows" n1 n2) in
  let data = floats_to_bigstring [| 1.0; 2.0; 3.0 |] in
  let vec_id =
    with_txn db (fun txn ->
        ok_exn (Gvecdb.create_edge_vector db ~txn edge "embedding" data))
  in
  check bool "exists before delete" true
    (ok_exn (Gvecdb.vector_exists db vec_id));
  with_txn db (fun txn -> ok_exn (Gvecdb.delete_vector db ~txn vec_id));
  check bool "not exists after delete" false
    (ok_exn (Gvecdb.vector_exists db vec_id))

let test_get_vectors_for_edge () =
  with_temp_db "edge_vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "person") in
  let n2 = ok_exn (Gvecdb.create_node db "person") in
  let edge = ok_exn (Gvecdb.create_edge db "knows" n1 n2) in
  let v1, v2 =
    with_txn db (fun txn ->
        let v1 =
          ok_exn
            (Gvecdb.create_edge_vector db ~txn edge "embedding1"
               (floats_to_bigstring [| 1.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_edge_vector db ~txn edge "embedding2"
               (floats_to_bigstring [| 2.0 |]))
        in
        (v1, v2))
  in
  let all_vecs = ok_exn (Gvecdb.get_vectors_for_edge db edge ()) in
  check int "two vectors" 2 (List.length all_vecs);
  let ids = List.map (fun (v : Gvecdb.vector_info) -> v.vector_id) all_vecs in
  check bool "v1 in list" true (List.mem v1 ids);
  check bool "v2 in list" true (List.mem v2 ids)

let test_get_vectors_for_edge_by_tag () =
  with_temp_db "edge_vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "person") in
  let n2 = ok_exn (Gvecdb.create_node db "person") in
  let edge = ok_exn (Gvecdb.create_edge db "knows" n1 n2) in
  let v2 =
    with_txn db (fun txn ->
        let _ =
          ok_exn
            (Gvecdb.create_edge_vector db ~txn edge "type_a"
               (floats_to_bigstring [| 1.0 |]))
        in
        let v2 =
          ok_exn
            (Gvecdb.create_edge_vector db ~txn edge "type_b"
               (floats_to_bigstring [| 2.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_edge_vector db ~txn edge "type_a"
               (floats_to_bigstring [| 3.0 |]))
        in
        v2)
  in
  let type_b_vecs =
    ok_exn (Gvecdb.get_vectors_for_edge db edge ~vector_tag:"type_b" ())
  in
  check int "one type_b vector" 1 (List.length type_b_vecs);
  let first_vec : Gvecdb.vector_info = List.hd type_b_vecs in
  check int64 "correct id" v2 first_vec.vector_id

let test_knn_mixed_node_edge_vectors () =
  with_temp_db "edge_vectors" @@ fun db ->
  let n1 = ok_exn (Gvecdb.create_node db "doc") in
  let n2 = ok_exn (Gvecdb.create_node db "doc") in
  let edge = ok_exn (Gvecdb.create_edge db "related" n1 n2) in
  (* node vector at (1,0), edge vector at (0,0), node vector at (10,10) *)
  (* edge vector should be nearest to query (0.1, 0.0) *)
  let v_edge =
    with_txn db (fun txn ->
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n1 "e"
               (floats_to_bigstring [| 1.0; 0.0 |]))
        in
        let v_edge =
          ok_exn
            (Gvecdb.create_edge_vector db ~txn edge "e"
               (floats_to_bigstring [| 0.0; 0.0 |]))
        in
        let _ =
          ok_exn
            (Gvecdb.create_vector db ~txn n2 "e"
               (floats_to_bigstring [| 10.0; 10.0 |]))
        in
        v_edge)
  in
  (* query (0.1, 0.0) - closest to (0,0) which is the edge vector *)
  let query = [| 0.1; 0.0 |] in
  let results =
    ok_exn (Gvecdb.knn_brute_force db ~metric:Euclidean ~k:3 query)
  in
  check int "three results" 3 (List.length results);
  (* first result should be edge vector (0,0) - nearest to query *)
  let r1 = List.hd results in
  check int64 "nearest is edge vector" v_edge r1.Gvecdb.vector_id;
  check bool "first is Edge" true (r1.Gvecdb.owner_kind = Gvecdb.Edge);
  (* verify that we get a mix of node and edge owners in results *)
  let has_node =
    List.exists (fun r -> r.Gvecdb.owner_kind = Gvecdb.Node) results
  in
  let has_edge =
    List.exists (fun r -> r.Gvecdb.owner_kind = Gvecdb.Edge) results
  in
  check bool "results include node vectors" true has_node;
  check bool "results include edge vectors" true has_edge

(** {1 Test runner} *)

let crud_tests =
  [
    ("create_vector", `Quick, test_create_vector);
    ("get_vector", `Quick, test_get_vector);
    ("get_vector_unnormalized", `Quick, test_get_vector_unnormalized);
    ("get_vector_info", `Quick, test_get_vector_info);
    ("delete_vector", `Quick, test_delete_vector);
    ("vector_not_found", `Quick, test_vector_not_found);
    ("create_vector_node_not_found", `Quick, test_create_vector_node_not_found);
  ]

let multi_vector_tests =
  [
    ("multiple_vectors_per_node", `Quick, test_multiple_vectors_per_node);
    ("get_vectors_by_tag", `Quick, test_get_vectors_by_tag);
    ("get_vectors_nonexistent_tag", `Quick, test_get_vectors_nonexistent_tag);
    ("vector_ids_sequential", `Quick, test_vector_ids_sequential);
  ]

let knn_tests =
  [
    ("knn_euclidean", `Quick, test_knn_euclidean);
    ("knn_cosine", `Quick, test_knn_cosine);
    ("knn_dot_product", `Quick, test_knn_dot_product);
    ("knn_empty_db", `Quick, test_knn_empty_db);
    ("knn_cosine_zero_vector", `Quick, test_knn_cosine_zero_vector);
    ("knn_dimension_mismatch", `Quick, test_knn_dimension_mismatch);
    ("knn_k_larger_than_db", `Quick, test_knn_k_larger_than_db);
    ("knn_brute_force_bs", `Quick, test_knn_brute_force_bs);
    ("knn_mixed_normalized", `Quick, test_knn_mixed_normalized);
  ]

let edge_vector_tests =
  [
    ("create_edge_vector", `Quick, test_create_edge_vector);
    ("get_edge_vector_info", `Quick, test_get_edge_vector_info);
    ("get_edge_vector_data", `Quick, test_get_edge_vector_data);
    ( "create_edge_vector_edge_not_found",
      `Quick,
      test_create_edge_vector_edge_not_found );
    ("delete_edge_vector", `Quick, test_delete_edge_vector);
    ("get_vectors_for_edge", `Quick, test_get_vectors_for_edge);
    ("get_vectors_for_edge_by_tag", `Quick, test_get_vectors_for_edge_by_tag);
    ("knn_mixed_node_edge_vectors", `Quick, test_knn_mixed_node_edge_vectors);
  ]

let () =
  run "Vectors"
    [
      ("crud", crud_tests);
      ("multi_vector", multi_vector_tests);
      ("knn", knn_tests);
      ("edge_vectors", edge_vector_tests);
    ]
