module Api = Gvecdb_api.MakeRPC (Capnp_rpc)
module G = Api.Client.Gvecdb

let create_node t node_type =
  let open G.CreateNode in
  let request, params =
    Capnp_rpc.Capability.Request.create Params.init_pointer
  in
  Params.node_type_set params node_type;
  let result = Capnp_rpc.Capability.call_for_value_exn t method_id request in
  let err = Results.error_get result in
  if String.length err > 0 then Error err else Ok (Results.node_id_get result)

let create_edge t edge_type src dst =
  let open G.CreateEdge in
  let request, params =
    Capnp_rpc.Capability.Request.create Params.init_pointer
  in
  Params.edge_type_set params edge_type;
  Params.src_set params src;
  Params.dst_set params dst;
  let result = Capnp_rpc.Capability.call_for_value_exn t method_id request in
  let err = Results.error_get result in
  if String.length err > 0 then Error err else Ok (Results.edge_id_get result)

let set_node_props t node_id node_type props =
  let open G.SetNodeProps in
  let request, params =
    Capnp_rpc.Capability.Request.create Params.init_pointer
  in
  Params.node_id_set params node_id;
  Params.node_type_set params node_type;
  Params.props_set params props;
  let result = Capnp_rpc.Capability.call_for_value_exn t method_id request in
  let err = Results.error_get result in
  if String.length err > 0 then Error err else Ok ()

let get_node_props t node_id =
  let open G.GetNodeProps in
  let request, params =
    Capnp_rpc.Capability.Request.create Params.init_pointer
  in
  Params.node_id_set params node_id;
  let result = Capnp_rpc.Capability.call_for_value_exn t method_id request in
  let err = Results.error_get result in
  if String.length err > 0 then Error err else Ok (Results.props_get result)

let read_node_field t node_id field_name =
  let open G.ReadNodeField in
  let request, params =
    Capnp_rpc.Capability.Request.create Params.init_pointer
  in
  Params.node_id_set params node_id;
  Params.field_name_set params field_name;
  let result = Capnp_rpc.Capability.call_for_value_exn t method_id request in
  let err = Results.error_get result in
  if String.length err > 0 then Error err else Ok (Results.value_get result)

let register_schema_from_capnp t ~kind ~type_name ~capnp_data ~struct_name =
  let open G.RegisterSchemaFromCapnp in
  let request, params =
    Capnp_rpc.Capability.Request.create Params.init_pointer
  in
  Params.kind_set_exn params kind;
  Params.type_name_set params type_name;
  Params.capnp_schema_set params capnp_data;
  Params.struct_name_set params struct_name;
  let result = Capnp_rpc.Capability.call_for_value_exn t method_id request in
  let err = Results.error_get result in
  if String.length err > 0 then Error err else Ok ()

let get_outbound_edges t node_id =
  let open G.GetOutboundEdges in
  let request, params =
    Capnp_rpc.Capability.Request.create Params.init_pointer
  in
  Params.node_id_set params node_id;
  let result = Capnp_rpc.Capability.call_for_value_exn t method_id request in
  let err = Results.error_get result in
  if String.length err > 0 then Error err
  else
    let edges = Results.edges_get_list result in
    Ok
      (List.map
         (fun e ->
           ( Api.Reader.EdgeInfo.id_get e,
             Api.Reader.EdgeInfo.edge_type_get e,
             Api.Reader.EdgeInfo.src_get e,
             Api.Reader.EdgeInfo.dst_get e ))
         edges)

let get_outbound_edges_filtered t node_id ~edge_type ~filters =
  let open G.GetOutboundEdgesFiltered in
  let request, params =
    Capnp_rpc.Capability.Request.create Params.init_pointer
  in
  Params.node_id_set params node_id;
  Params.edge_type_set params edge_type;
  let filter_arr = Params.filters_init params (List.length filters) in
  List.iteri
    (fun i (field_name, op, value_data) ->
      let fb = Capnp.Array.get filter_arr i in
      Api.Builder.FilterPredicate.field_name_set fb field_name;
      Api.Builder.FilterPredicate.op_set_exn fb op;
      Api.Builder.FilterPredicate.value_set fb value_data)
    filters;
  let result = Capnp_rpc.Capability.call_for_value_exn t method_id request in
  let err = Results.error_get result in
  if String.length err > 0 then Error err
  else
    let edges = Results.edges_get_list result in
    Ok
      (List.map
         (fun e ->
           ( Api.Reader.EdgeInfo.id_get e,
             Api.Reader.EdgeInfo.edge_type_get e,
             Api.Reader.EdgeInfo.src_get e,
             Api.Reader.EdgeInfo.dst_get e ))
         edges)
