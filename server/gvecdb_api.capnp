@0xa1b2c3d4e5f60718;

struct FieldDescriptor {
  name @0 :Text;
  type @1 :UInt8;
  # 0=void, 1=bool, 2=int8, 3=int16, 4=int32, 5=int64,
  # 6=uint8, 7=uint16, 8=uint32, 9=uint64, 10=float32, 11=float64,
  # 12=text, 13=data
  offset @2 :UInt32;
  isPointer @3 :Bool;
}

struct FilterPredicate {
  fieldName @0 :Text;
  op @1 :UInt8;
  # 0=eq, 1=neq, 2=lt, 3=gt, 4=lte, 5=gte
  value @2 :Data;
  # serialized field_value: 1 byte type tag + value bytes
}

struct KnnResult {
  vectorId @0 :Int64;
  ownerKind @1 :UInt8;
  # 0=node, 1=edge
  ownerId @2 :Int64;
  vectorTag @3 :Text;
  distance @4 :Float64;
}

struct EdgeInfo {
  id @0 :Int64;
  edgeType @1 :Text;
  src @2 :Int64;
  dst @3 :Int64;
}

struct NodeInfo {
  id @0 :Int64;
  nodeType @1 :Text;
}

interface Gvecdb {
  # -- Schema Registration --
  registerSchemaFromCapnp @0 (
    kind :UInt8,
    typeName :Text,
    capnpSchema :Data,
    structName :Text
  ) -> (success :Bool, error :Text);

  registerSchemaFromFields @1 (
    kind :UInt8,
    typeName :Text,
    dataWordCount :UInt16,
    pointerCount :UInt16,
    fields :List(FieldDescriptor)
  ) -> (success :Bool, error :Text);

  # -- Node CRUD --
  createNode @2 (nodeType :Text) -> (nodeId :Int64, error :Text);
  deleteNode @3 (nodeId :Int64) -> (success :Bool, error :Text);
  getNodeInfo @4 (nodeId :Int64) -> (info :NodeInfo, error :Text);

  # -- Edge CRUD --
  createEdge @5 (edgeType :Text, src :Int64, dst :Int64) -> (edgeId :Int64, error :Text);
  deleteEdge @6 (edgeId :Int64) -> (success :Bool, error :Text);
  getEdgeInfo @7 (edgeId :Int64) -> (info :EdgeInfo, error :Text);

  # -- Properties (opaque Data on wire, schema-aware on server) --
  setNodeProps @8 (nodeId :Int64, nodeType :Text, props :Data) -> (success :Bool, error :Text);
  getNodeProps @9 (nodeId :Int64) -> (props :Data, error :Text);
  setEdgeProps @10 (edgeId :Int64, edgeType :Text, props :Data) -> (success :Bool, error :Text);
  getEdgeProps @11 (edgeId :Int64) -> (props :Data, error :Text);

  # -- Dynamic Property Read --
  readNodeField @12 (nodeId :Int64, fieldName :Text) -> (value :Data, error :Text);
  readEdgeField @13 (edgeId :Int64, fieldName :Text) -> (value :Data, error :Text);

  # -- Adjacency Queries --
  getOutboundEdges @14 (nodeId :Int64) -> (edges :List(EdgeInfo), error :Text);
  getInboundEdges @15 (nodeId :Int64) -> (edges :List(EdgeInfo), error :Text);
  getOutboundEdgesFiltered @16 (
    nodeId :Int64,
    edgeType :Text,
    filters :List(FilterPredicate)
  ) -> (edges :List(EdgeInfo), error :Text);

  # -- Vector Operations --
  createVector @17 (
    nodeId :Int64,
    vectorTag :Text,
    data :Data,
    normalize :Bool,
    metric :UInt8
  ) -> (vectorId :Int64, error :Text);
  deleteVector @18 (vectorId :Int64) -> (success :Bool, error :Text);

  # -- k-NN Search --
  knnHnsw @19 (
    vectorTag :Text,
    query :Data,
    k :UInt32,
    ef :UInt32,
    metric :UInt8
  ) -> (results :List(KnnResult), error :Text);

  knnBruteForce @20 (
    query :Data,
    k :UInt32,
    metric :UInt8
  ) -> (results :List(KnnResult), error :Text);

  # -- Maintenance --
  rebuildHnswIndex @21 (
    vectorTag :Text
  ) -> (success :Bool, error :Text);
}
