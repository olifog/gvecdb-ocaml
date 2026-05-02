module Api = Gvecdb_api.MakeRPC (Capnp_rpc)
module Bigstring = Bigstringaf

let set_edge_info_fields (ei : Gvecdb.edge_info) (b : Api.Builder.EdgeInfo.t) =
  Api.Builder.EdgeInfo.id_set b ei.id;
  Api.Builder.EdgeInfo.edge_type_set b ei.edge_type;
  Api.Builder.EdgeInfo.src_set b ei.src;
  Api.Builder.EdgeInfo.dst_set b ei.dst

let set_knn_result_fields (r : Gvecdb.knn_result) (b : Api.Builder.KnnResult.t)
    =
  Api.Builder.KnnResult.vector_id_set b r.vector_id;
  Api.Builder.KnnResult.owner_kind_set_exn b
    (match r.owner_kind with Gvecdb.Node -> 0 | Gvecdb.Edge -> 1);
  Api.Builder.KnnResult.owner_id_set b r.owner_id;
  Api.Builder.KnnResult.vector_tag_set b r.vector_tag;
  Api.Builder.KnnResult.distance_set b r.distance

let set_edge_list results edges init_fn =
  let arr = init_fn results (List.length edges) in
  List.iteri (fun i ei -> set_edge_info_fields ei (Capnp.Array.get arr i)) edges

let metric_of_uint8 = function
  | 0 -> Gvecdb.Euclidean
  | 1 -> Gvecdb.Cosine
  | 2 -> Gvecdb.DotProduct
  | n -> failwith (Printf.sprintf "invalid metric: %d" n)

let kind_of_uint8 = function
  | 0 -> Gvecdb.Schema_registry.NodeSchemaKind
  | 1 -> Gvecdb.Schema_registry.EdgeSchemaKind
  | n -> failwith (Printf.sprintf "invalid schema kind: %d" n)

let serialize_field_value (v : Gvecdb.Dynamic_reader.field_value) : string =
  let open Gvecdb.Dynamic_reader in
  let buf = Buffer.create 16 in
  (match v with
  | V_void -> Buffer.add_uint8 buf 0
  | V_bool b ->
      Buffer.add_uint8 buf 1;
      Buffer.add_uint8 buf (if b then 1 else 0)
  | V_int8 v ->
      Buffer.add_uint8 buf 2;
      Buffer.add_uint8 buf (v land 0xFF)
  | V_int16 v ->
      Buffer.add_uint8 buf 3;
      Buffer.add_uint16_le buf (v land 0xFFFF)
  | V_int32 v ->
      Buffer.add_uint8 buf 4;
      Buffer.add_int32_le buf v
  | V_int64 v ->
      Buffer.add_uint8 buf 5;
      Buffer.add_int64_le buf v
  | V_uint8 v ->
      Buffer.add_uint8 buf 6;
      Buffer.add_uint8 buf v
  | V_uint16 v ->
      Buffer.add_uint8 buf 7;
      Buffer.add_uint16_le buf v
  | V_uint32 v ->
      Buffer.add_uint8 buf 8;
      Buffer.add_int32_le buf v
  | V_uint64 v ->
      Buffer.add_uint8 buf 9;
      Buffer.add_int64_le buf v
  | V_float32 v ->
      Buffer.add_uint8 buf 10;
      Buffer.add_int32_le buf (Int32.bits_of_float v)
  | V_float64 v ->
      Buffer.add_uint8 buf 11;
      Buffer.add_int64_le buf (Int64.bits_of_float v)
  | V_text s ->
      Buffer.add_uint8 buf 12;
      Buffer.add_string buf s
  | V_data s ->
      Buffer.add_uint8 buf 13;
      Buffer.add_string buf s);
  Buffer.contents buf

let deserialize_field_value (s : string) : Gvecdb.Dynamic_reader.field_value =
  let open Gvecdb.Dynamic_reader in
  let len = String.length s in
  if len = 0 then V_void
  else
    match Char.code s.[0] with
    | 1 -> V_bool (len > 1 && Char.code s.[1] = 1)
    | 2 when len > 1 ->
        let v = Char.code s.[1] in
        V_int8 (if v > 127 then v - 256 else v)
    | 3 when len > 2 ->
        let v = Char.code s.[1] lor (Char.code s.[2] lsl 8) in
        V_int16 (if v > 32767 then v - 65536 else v)
    | 4 when len > 4 -> V_int32 (String.get_int32_le s 1)
    | 5 when len > 8 -> V_int64 (String.get_int64_le s 1)
    | 6 when len > 1 -> V_uint8 (Char.code s.[1])
    | 7 when len > 2 -> V_uint16 (Char.code s.[1] lor (Char.code s.[2] lsl 8))
    | 8 when len > 4 -> V_uint32 (String.get_int32_le s 1)
    | 9 when len > 8 -> V_uint64 (String.get_int64_le s 1)
    | 10 when len > 4 ->
        V_float32 (Int32.float_of_bits (String.get_int32_le s 1))
    | 11 when len > 8 ->
        V_float64 (Int64.float_of_bits (String.get_int64_le s 1))
    | 12 -> V_text (String.sub s 1 (len - 1))
    | 13 -> V_data (String.sub s 1 (len - 1))
    | _ -> V_void

let local (db : Gvecdb.t) =
  let module G = Api.Service.Gvecdb in
  G.local
  @@ object
       inherit G.service

       method register_schema_from_capnp_impl params release_param_caps =
         let open G.RegisterSchemaFromCapnp in
         let kind = kind_of_uint8 (Params.kind_get params) in
         let type_name = Params.type_name_get params in
         let capnp_data = Params.capnp_schema_get params in
         let struct_name = Params.struct_name_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         let tmp = Filename.temp_file "gvecdb_schema" ".capnp" in
         Fun.protect
           ~finally:(fun () -> try Sys.remove tmp with Sys_error _ -> ())
           (fun () ->
             let oc = open_out_bin tmp in
             output_string oc capnp_data;
             close_out oc;
             match
               Gvecdb.register_schema_from_capnp db ~kind ~type_name
                 ~capnp_path:tmp ~struct_name ()
             with
             | Ok _ -> Results.success_set results true
             | Error e ->
                 Results.success_set results false;
                 Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method register_schema_from_fields_impl params release_param_caps =
         let open G.RegisterSchemaFromFields in
         let kind = kind_of_uint8 (Params.kind_get params) in
         let type_name = Params.type_name_get params in
         let dwc = Params.data_word_count_get params in
         let pc = Params.pointer_count_get params in
         let field_arr = Params.fields_get_array params in
         release_param_caps ();
         let fields =
           Array.to_list
             (Array.map
                (fun fd ->
                  let open Gvecdb.Schema_registry in
                  let name = Api.Reader.FieldDescriptor.name_get fd in
                  let ft =
                    field_type_of_int (Api.Reader.FieldDescriptor.type_get fd)
                  in
                  let offset =
                    Stdint.Uint32.to_int
                      (Api.Reader.FieldDescriptor.offset_get fd)
                  in
                  let is_pointer =
                    Api.Reader.FieldDescriptor.is_pointer_get fd
                  in
                  {
                    name;
                    field_type = ft;
                    offset;
                    is_pointer;
                    default_value = No_default;
                  })
                field_arr)
         in
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match
            Gvecdb.register_schema_from_fields db ~kind ~type_name
              ~data_word_count:dwc ~pointer_count:pc ~fields ()
          with
         | Ok _ -> Results.success_set results true
         | Error e ->
             Results.success_set results false;
             Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method create_node_impl params release_param_caps =
         let open G.CreateNode in
         let node_type = Params.node_type_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.create_node db node_type with
         | Ok id -> Results.node_id_set results id
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method delete_node_impl params release_param_caps =
         let open G.DeleteNode in
         let node_id = Params.node_id_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.delete_node db node_id with
         | Ok () -> Results.success_set results true
         | Error e ->
             Results.success_set results false;
             Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method get_node_info_impl params release_param_caps =
         let open G.GetNodeInfo in
         let node_id = Params.node_id_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.get_node_info db node_id with
         | Ok info ->
             let b = Results.info_init results in
             Api.Builder.NodeInfo.id_set b info.id;
             Api.Builder.NodeInfo.node_type_set b info.node_type
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method create_edge_impl params release_param_caps =
         let open G.CreateEdge in
         let edge_type = Params.edge_type_get params in
         let src = Params.src_get params in
         let dst = Params.dst_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.create_edge db edge_type src dst with
         | Ok id -> Results.edge_id_set results id
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method delete_edge_impl params release_param_caps =
         let open G.DeleteEdge in
         let edge_id = Params.edge_id_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.delete_edge db edge_id with
         | Ok () -> Results.success_set results true
         | Error e ->
             Results.success_set results false;
             Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method get_edge_info_impl params release_param_caps =
         let open G.GetEdgeInfo in
         let edge_id = Params.edge_id_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.get_edge_info db edge_id with
         | Ok info ->
             let b = Results.info_init results in
             set_edge_info_fields info b
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method set_node_props_impl params release_param_caps =
         let open G.SetNodeProps in
         let node_id = Params.node_id_get params in
         let node_type = Params.node_type_get params in
         let props = Params.props_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         let bs = Bigstring.of_string ~off:0 ~len:(String.length props) props in
         (match Gvecdb.set_node_props db node_id node_type bs with
         | Ok () -> Results.success_set results true
         | Error e ->
             Results.success_set results false;
             Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method get_node_props_impl params release_param_caps =
         let open G.GetNodeProps in
         let node_id = Params.node_id_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.get_node_props db node_id with
         | Ok bs -> Results.props_set results (Bigstring.to_string bs)
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method set_edge_props_impl params release_param_caps =
         let open G.SetEdgeProps in
         let edge_id = Params.edge_id_get params in
         let _edge_type = Params.edge_type_get params in
         let props = Params.props_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         let bs = Bigstring.of_string ~off:0 ~len:(String.length props) props in
         (match Gvecdb.set_edge_props db edge_id bs with
         | Ok () -> Results.success_set results true
         | Error e ->
             Results.success_set results false;
             Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method get_edge_props_impl params release_param_caps =
         let open G.GetEdgeProps in
         let edge_id = Params.edge_id_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.get_edge_props db edge_id with
         | Ok bs -> Results.props_set results (Bigstring.to_string bs)
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method read_node_field_impl params release_param_caps =
         let open G.ReadNodeField in
         let node_id = Params.node_id_get params in
         let field_name = Params.field_name_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.read_node_field db node_id field_name with
         | Ok v -> Results.value_set results (serialize_field_value v)
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method read_edge_field_impl params release_param_caps =
         let open G.ReadEdgeField in
         let edge_id = Params.edge_id_get params in
         let field_name = Params.field_name_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.read_edge_field db edge_id field_name with
         | Ok v -> Results.value_set results (serialize_field_value v)
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method get_outbound_edges_impl params release_param_caps =
         let open G.GetOutboundEdges in
         let node_id = Params.node_id_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.get_outbound_edges db node_id () with
         | Ok edges -> set_edge_list results edges Results.edges_init
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method get_inbound_edges_impl params release_param_caps =
         let open G.GetInboundEdges in
         let node_id = Params.node_id_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match Gvecdb.get_inbound_edges db node_id () with
         | Ok edges -> set_edge_list results edges Results.edges_init
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method get_outbound_edges_filtered_impl params release_param_caps =
         let open G.GetOutboundEdgesFiltered in
         let node_id = Params.node_id_get params in
         let edge_type = Params.edge_type_get params in
         let filter_arr = Params.filters_get_array params in
         release_param_caps ();
         let filters =
           Array.to_list
             (Array.map
                (fun fp ->
                  let field_name =
                    Api.Reader.FilterPredicate.field_name_get fp
                  in
                  let op =
                    match Api.Reader.FilterPredicate.op_get fp with
                    | 0 -> Gvecdb.Filter.Eq
                    | 1 -> Gvecdb.Filter.Neq
                    | 2 -> Gvecdb.Filter.Lt
                    | 3 -> Gvecdb.Filter.Gt
                    | 4 -> Gvecdb.Filter.Lte
                    | 5 -> Gvecdb.Filter.Gte
                    | n -> failwith (Printf.sprintf "invalid filter op: %d" n)
                  in
                  let value =
                    deserialize_field_value
                      (Api.Reader.FilterPredicate.value_get fp)
                  in
                  Gvecdb.Filter.{ field_name; op; value })
                filter_arr)
         in
         let edge_type_opt =
           if String.length edge_type = 0 then None else Some edge_type
         in
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match
            Gvecdb.get_outbound_edges db node_id ?edge_type:edge_type_opt
              ~filters ()
          with
         | Ok edges -> set_edge_list results edges Results.edges_init
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method create_vector_impl params release_param_caps =
         let open G.CreateVector in
         let node_id = Params.node_id_get params in
         let vector_tag = Params.vector_tag_get params in
         let data = Params.data_get params in
         let normalize = Params.normalize_get params in
         let metric = metric_of_uint8 (Params.metric_get params) in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         let bs = Bigstring.of_string ~off:0 ~len:(String.length data) data in
         (match
            Gvecdb.with_transaction db (fun txn ->
                Gvecdb.create_vector db ~txn ~normalize ~metric Node node_id
                  vector_tag bs)
          with
         | Some (Ok vid) -> Results.vector_id_set results vid
         | Some (Error e) ->
             Results.error_set results (Gvecdb.Error.to_string e)
         | None -> Results.error_set results "transaction aborted");
         Capnp_rpc.Service.return response

       method create_vector_batch_impl params release_param_caps =
         let open G.CreateVectorBatch in
         let node_ids = Params.node_ids_get_array params in
         let vector_tag = Params.vector_tag_get params in
         let vectors = Params.vectors_get_array params in
         let normalize = Params.normalize_get params in
         let metric = metric_of_uint8 (Params.metric_get params) in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         let requests =
           Array.to_list
             (Array.mapi
                (fun i node_id ->
                  let data = vectors.(i) in
                  let bs =
                    Bigstring.of_string ~off:0 ~len:(String.length data) data
                  in
                  Gvecdb.
                    {
                      owner_kind = Node;
                      owner_id = node_id;
                      vector_tag;
                      data = bs;
                      normalize;
                      metric;
                    })
                node_ids)
         in
         (match
            Gvecdb.with_transaction db (fun txn ->
                Gvecdb.create_vectors_batch db ~txn requests)
          with
         | Some (Ok ids) ->
             let arr = Results.vector_ids_init results (List.length ids) in
             List.iteri (fun i vid -> Capnp.Array.set arr i vid) ids
         | Some (Error e) ->
             Results.error_set results (Gvecdb.Error.to_string e)
         | None -> Results.error_set results "transaction aborted");
         Capnp_rpc.Service.return response

       method delete_vector_impl params release_param_caps =
         let open G.DeleteVector in
         let vector_id = Params.vector_id_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match
            Gvecdb.with_transaction db (fun txn ->
                Gvecdb.delete_vector db ~txn vector_id)
          with
         | Some (Ok ()) -> Results.success_set results true
         | Some (Error e) ->
             Results.success_set results false;
             Results.error_set results (Gvecdb.Error.to_string e)
         | None ->
             Results.success_set results false;
             Results.error_set results "transaction aborted");
         Capnp_rpc.Service.return response

       method knn_hnsw_impl params release_param_caps =
         let open G.KnnHnsw in
         let vector_tag = Params.vector_tag_get params in
         let query_data = Params.query_get params in
         let k = Stdint.Uint32.to_int (Params.k_get params) in
         let ef = Stdint.Uint32.to_int (Params.ef_get params) in
         let metric = metric_of_uint8 (Params.metric_get params) in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         let bs =
           Bigstring.of_string ~off:0 ~len:(String.length query_data) query_data
         in
         let dim = Bigstring.length bs / 4 in
         let query =
           Array.init dim (fun i ->
               Int32.float_of_bits (Bigstring.get_int32_le bs (i * 4)))
         in
         (match Gvecdb.knn_hnsw db ~metric ~k ~ef ~vector_tag query with
         | Ok knn_results ->
             let arr = Results.results_init results (List.length knn_results) in
             List.iteri
               (fun i r -> set_knn_result_fields r (Capnp.Array.get arr i))
               knn_results
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response

       method rebuild_hnsw_index_impl params release_param_caps =
         let open G.RebuildHnswIndex in
         let vector_tag = Params.vector_tag_get params in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         (match
            Gvecdb.with_transaction db (fun txn ->
                Gvecdb.rebuild_hnsw_index db ~txn ~vector_tag ())
          with
         | Some (Ok ()) -> Results.success_set results true
         | Some (Error e) ->
             Results.success_set results false;
             Results.error_set results (Gvecdb.Error.to_string e)
         | None ->
             Results.success_set results false;
             Results.error_set results "transaction aborted");
         Capnp_rpc.Service.return response

       method knn_brute_force_impl params release_param_caps =
         let open G.KnnBruteForce in
         let query_data = Params.query_get params in
         let k = Stdint.Uint32.to_int (Params.k_get params) in
         let metric = metric_of_uint8 (Params.metric_get params) in
         release_param_caps ();
         let response, results =
           Capnp_rpc.Service.Response.create Results.init_pointer
         in
         let bs =
           Bigstring.of_string ~off:0 ~len:(String.length query_data) query_data
         in
         let dim = Bigstring.length bs / 4 in
         let query =
           Array.init dim (fun i ->
               Int32.float_of_bits (Bigstring.get_int32_le bs (i * 4)))
         in
         (match Gvecdb.knn_brute_force db ~metric ~k query with
         | Ok knn_results ->
             let arr = Results.results_init results (List.length knn_results) in
             List.iteri
               (fun i r -> set_knn_result_fields r (Capnp.Array.get arr i))
               knn_results
         | Error e -> Results.error_set results (Gvecdb.Error.to_string e));
         Capnp_rpc.Service.return response
     end
