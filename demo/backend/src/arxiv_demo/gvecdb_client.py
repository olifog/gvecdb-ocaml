from __future__ import annotations

import struct
from pathlib import Path
from typing import Any, TypedDict

import capnp


class PaperProps(TypedDict):
    title: str
    abstract: str
    year: int
    arxiv_id: str
    categories: str
    doi: str
    journal_ref: str
    submitted_date: str
    page_count: int
    figure_count: int
    version_count: int
    comments: str


class AuthorProps(TypedDict):
    name: str
    paper_count: int


class AuthoredProps(TypedDict):
    position: int


SCHEMAS_DIR = Path(__file__).resolve().parent.parent.parent / "schemas"
API_SCHEMA = SCHEMAS_DIR / "gvecdb_api.capnp"
ARXIV_SCHEMA = SCHEMAS_DIR / "arxiv.capnp"

gvecdb_api = capnp.load(str(API_SCHEMA))  # type: ignore[no-untyped-call]
arxiv_schema = capnp.load(str(ARXIV_SCHEMA))  # type: ignore[no-untyped-call]

NODE_SCHEMA_KIND = 0
EDGE_SCHEMA_KIND = 1

METRIC_EUCLIDEAN = 0
METRIC_COSINE = 1
METRIC_DOT_PRODUCT = 2


class EdgeInfo:
    __slots__ = ("id", "edge_type", "src", "dst")

    def __init__(self, id: int, edge_type: str, src: int, dst: int) -> None:
        self.id = id
        self.edge_type = edge_type
        self.src = src
        self.dst = dst


class NodeInfo:
    __slots__ = ("id", "node_type")

    def __init__(self, id: int, node_type: str) -> None:
        self.id = id
        self.node_type = node_type


class KnnResult:
    __slots__ = ("vector_id", "owner_kind", "owner_id", "vector_tag", "distance")

    def __init__(
        self,
        vector_id: int,
        owner_kind: int,
        owner_id: int,
        vector_tag: str,
        distance: float,
    ) -> None:
        self.vector_id = vector_id
        self.owner_kind = owner_kind
        self.owner_id = owner_id
        self.vector_tag = vector_tag
        self.distance = distance


def _check_error(result: Any) -> None:
    err = result.error
    if err:
        raise RuntimeError(f"gvecdb error: {err}")


def pack_float32_vector(vec: list[float]) -> bytes:
    return struct.pack(f"<{len(vec)}f", *vec)


class GvecdbClient:
    def __init__(self, client: Any, connection: Any) -> None:
        self._client = client
        self._connection = connection

    @classmethod
    async def connect(cls, socket_path: str) -> GvecdbClient:
        conn = await capnp.AsyncIoStream.create_unix_connection(  # type: ignore[no-untyped-call]
            path=socket_path
        )
        tpc = capnp.TwoPartyClient(conn)  # type: ignore[no-untyped-call]
        bootstrap: Any = tpc.bootstrap().cast_as(gvecdb_api.Gvecdb)  # type: ignore[no-untyped-call]
        return cls(bootstrap, conn)

    async def register_schemas(self) -> None:
        schema_data = ARXIV_SCHEMA.read_bytes()
        registrations = [
            (NODE_SCHEMA_KIND, "paper", "Paper"),
            (NODE_SCHEMA_KIND, "author", "Author"),
            (EDGE_SCHEMA_KIND, "authored", "Authored"),
            (EDGE_SCHEMA_KIND, "cites", "Cites"),
        ]
        for kind, type_name, struct_name in registrations:
            result = await self._client.registerSchemaFromCapnp(
                kind=kind,
                typeName=type_name,
                capnpSchema=schema_data,
                structName=struct_name,
            )
            _check_error(result)

    async def create_node(self, node_type: str) -> int:
        result = await self._client.createNode(nodeType=node_type)
        _check_error(result)
        return int(result.nodeId)

    async def delete_node(self, node_id: int) -> None:
        result = await self._client.deleteNode(nodeId=node_id)
        _check_error(result)

    async def get_node_info(self, node_id: int) -> NodeInfo:
        result = await self._client.getNodeInfo(nodeId=node_id)
        _check_error(result)
        info = result.info
        return NodeInfo(id=int(info.id), node_type=str(info.nodeType))

    async def create_edge(self, edge_type: str, src: int, dst: int) -> int:
        result = await self._client.createEdge(edgeType=edge_type, src=src, dst=dst)
        _check_error(result)
        return int(result.edgeId)

    async def get_edge_info(self, edge_id: int) -> EdgeInfo:
        result = await self._client.getEdgeInfo(edgeId=edge_id)
        _check_error(result)
        info = result.info
        return EdgeInfo(
            id=int(info.id),
            edge_type=str(info.edgeType),
            src=int(info.src),
            dst=int(info.dst),
        )

    async def set_node_props(self, node_id: int, node_type: str, props_bytes: bytes) -> None:
        result = await self._client.setNodeProps(
            nodeId=node_id, nodeType=node_type, props=props_bytes
        )
        _check_error(result)

    async def get_node_props(self, node_id: int) -> bytes:
        result = await self._client.getNodeProps(nodeId=node_id)
        _check_error(result)
        return bytes(result.props)

    async def set_edge_props(self, edge_id: int, edge_type: str, props_bytes: bytes) -> None:
        result = await self._client.setEdgeProps(
            edgeId=edge_id, edgeType=edge_type, props=props_bytes
        )
        _check_error(result)

    async def get_edge_props(self, edge_id: int) -> bytes:
        result = await self._client.getEdgeProps(edgeId=edge_id)
        _check_error(result)
        return bytes(result.props)

    async def read_node_field(self, node_id: int, field_name: str) -> bytes:
        result = await self._client.readNodeField(nodeId=node_id, fieldName=field_name)
        _check_error(result)
        return bytes(result.value)

    async def read_edge_field(self, edge_id: int, field_name: str) -> bytes:
        result = await self._client.readEdgeField(edgeId=edge_id, fieldName=field_name)
        _check_error(result)
        return bytes(result.value)

    async def get_outbound_edges(self, node_id: int) -> list[EdgeInfo]:
        result = await self._client.getOutboundEdges(nodeId=node_id)
        _check_error(result)
        return [
            EdgeInfo(id=int(e.id), edge_type=str(e.edgeType), src=int(e.src), dst=int(e.dst))
            for e in result.edges
        ]

    async def get_inbound_edges(self, node_id: int) -> list[EdgeInfo]:
        result = await self._client.getInboundEdges(nodeId=node_id)
        _check_error(result)
        return [
            EdgeInfo(id=int(e.id), edge_type=str(e.edgeType), src=int(e.src), dst=int(e.dst))
            for e in result.edges
        ]

    async def create_vector(
        self,
        node_id: int,
        vector_tag: str,
        data: bytes,
        *,
        normalise: bool = False,
        metric: int = METRIC_COSINE,
    ) -> int:
        result = await self._client.createVector(
            nodeId=node_id,
            vectorTag=vector_tag,
            data=data,
            normalize=normalise,
            metric=metric,
        )
        _check_error(result)
        return int(result.vectorId)

    async def create_vector_batch(
        self,
        node_ids: list[int],
        vector_tag: str,
        vectors: list[bytes],
        *,
        normalise: bool = False,
        metric: int = METRIC_COSINE,
    ) -> list[int]:
        result = await self._client.createVectorBatch(
            nodeIds=node_ids,
            vectorTag=vector_tag,
            vectors=vectors,
            normalize=normalise,
            metric=metric,
        )
        _check_error(result)
        return [int(vid) for vid in result.vectorIds]

    async def create_vector_no_index(
        self,
        node_ids: list[int],
        vector_tag: str,
        vectors: list[bytes],
        *,
        normalise: bool = False,
        metric: int = METRIC_COSINE,
    ) -> list[int]:
        result = await self._client.createVectorNoIndex(
            nodeIds=node_ids,
            vectorTag=vector_tag,
            vectors=vectors,
            normalize=normalise,
            metric=metric,
        )
        _check_error(result)
        return [int(vid) for vid in result.vectorIds]

    async def rebuild_hnsw_index(self, vector_tag: str) -> None:
        result = await self._client.rebuildHnswIndex(vectorTag=vector_tag)
        _check_error(result)

    async def knn_hnsw(
        self,
        vector_tag: str,
        query: bytes,
        k: int = 10,
        ef: int = 64,
        metric: int = METRIC_COSINE,
    ) -> list[KnnResult]:
        result = await self._client.knnHnsw(
            vectorTag=vector_tag, query=query, k=k, ef=ef, metric=metric
        )
        _check_error(result)
        return [
            KnnResult(
                vector_id=int(r.vectorId),
                owner_kind=int(r.ownerKind),
                owner_id=int(r.ownerId),
                vector_tag=str(r.vectorTag),
                distance=float(r.distance),
            )
            for r in result.results
        ]

    @staticmethod
    def build_paper_props(
        title: str,
        abstract: str,
        year: int,
        arxiv_id: str,
        categories: str,
        doi: str = "",
        journal_ref: str = "",
        submitted_date: str = "",
        page_count: int = 0,
        figure_count: int = 0,
        version_count: int = 0,
        comments: str = "",
    ) -> bytes:
        paper = arxiv_schema.Paper.new_message()  # type: ignore[no-untyped-call]
        paper.title = title
        paper.abstract = abstract
        paper.year = year
        paper.arxivId = arxiv_id
        paper.categories = categories
        paper.doi = doi
        paper.journalRef = journal_ref
        paper.submittedDate = submitted_date
        paper.pageCount = page_count
        paper.figureCount = figure_count
        paper.versionCount = version_count
        paper.comments = comments
        return paper.to_bytes()  # type: ignore[no-any-return]

    @staticmethod
    def build_author_props(name: str, paper_count: int) -> bytes:
        author = arxiv_schema.Author.new_message()  # type: ignore[no-untyped-call]
        author.name = name
        author.paperCount = paper_count
        return author.to_bytes()  # type: ignore[no-any-return]

    @staticmethod
    def build_authored_props(position: int) -> bytes:
        authored = arxiv_schema.Authored.new_message()  # type: ignore[no-untyped-call]
        authored.position = position
        return authored.to_bytes()  # type: ignore[no-any-return]

    @staticmethod
    def build_cites_props(context: str = "") -> bytes:
        cites = arxiv_schema.Cites.new_message()  # type: ignore[no-untyped-call]
        cites.context = context
        return cites.to_bytes()  # type: ignore[no-any-return]

    @staticmethod
    def decode_paper_props(data: bytes) -> PaperProps:
        with arxiv_schema.Paper.from_bytes(data) as paper:  # type: ignore[no-untyped-call]
            return PaperProps(
                title=str(paper.title),
                abstract=str(paper.abstract),
                year=int(paper.year),
                arxiv_id=str(paper.arxivId),
                categories=str(paper.categories),
                doi=str(paper.doi),
                journal_ref=str(paper.journalRef),
                submitted_date=str(paper.submittedDate),
                page_count=int(paper.pageCount),
                figure_count=int(paper.figureCount),
                version_count=int(paper.versionCount),
                comments=str(paper.comments),
            )

    @staticmethod
    def decode_author_props(data: bytes) -> AuthorProps:
        with arxiv_schema.Author.from_bytes(data) as author:  # type: ignore[no-untyped-call]
            return AuthorProps(
                name=str(author.name),
                paper_count=int(author.paperCount),
            )

    @staticmethod
    def decode_authored_props(data: bytes) -> AuthoredProps:
        with arxiv_schema.Authored.from_bytes(data) as authored:  # type: ignore[no-untyped-call]
            return AuthoredProps(position=int(authored.position))

