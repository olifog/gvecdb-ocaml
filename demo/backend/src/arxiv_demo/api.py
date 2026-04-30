from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from typing import AsyncIterator

import capnp
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from arxiv_demo.embeddings import embed_to_bytes
from arxiv_demo.gvecdb_client import (
    EdgeInfo,
    GvecdbClient,
    KnnResult,
    METRIC_COSINE,
    PaperProps,
)
from arxiv_demo.models import (
    AuthorDetail,
    AuthorRef,
    GraphData,
    GraphEdge,
    GraphNode,
    PaperDetail,
    PaperRef,
    PaperSummary,
    SearchResponse,
)

logger = logging.getLogger(__name__)

SOCKET_PATH = os.environ.get("GVECDB_SOCKET", "/tmp/gvecdb.sock")
VALID_TAGS = {"abstract_embedding", "title_embedding"}
MAX_NEIGHBOURHOOD_NODES = 200

_client: GvecdbClient | None = None
_client_lock = asyncio.Lock()


async def get_client() -> GvecdbClient:
    global _client  # noqa: PLW0603
    if _client is not None:
        return _client
    async with _client_lock:
        if _client is not None:
            return _client
        _client = await GvecdbClient.connect(SOCKET_PATH)
        return _client


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    async with capnp.kj_loop():  # type: ignore[no-untyped-call]
        yield
    global _client  # noqa: PLW0603
    _client = None


app = FastAPI(title="arXiv gvecdb Demo", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _validate_tag(tag: str) -> None:
    if tag not in VALID_TAGS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid tag '{tag}'. Must be one of: {', '.join(sorted(VALID_TAGS))}",
        )


def _paper_summary_fields(data: PaperProps) -> dict[str, str | int]:
    return {
        "doi": data["doi"],
        "journal_ref": data["journal_ref"],
        "submitted_date": data["submitted_date"],
        "page_count": data["page_count"],
        "figure_count": data["figure_count"],
        "version_count": data["version_count"],
    }


async def _get_paper_authors(client: GvecdbClient, paper_node_id: int) -> list[AuthorRef]:
    inbound = await client.get_inbound_edges(paper_node_id)
    authored_edges = [e for e in inbound if e.edge_type == "authored"]
    if not authored_edges:
        return []

    async def _fetch_one(edge: EdgeInfo) -> AuthorRef | None:
        try:
            author_props, edge_props = await asyncio.gather(
                client.get_node_props(edge.src),
                client.get_edge_props(edge.id),
            )
            author_data = GvecdbClient.decode_author_props(author_props)
            authored_data = GvecdbClient.decode_authored_props(edge_props)
            return AuthorRef(
                node_id=edge.src,
                name=author_data["name"],
                position=authored_data["position"],
            )
        except RuntimeError:
            logger.warning("failed to fetch author for edge %d", edge.id)
            return None

    results = await asyncio.gather(*[_fetch_one(e) for e in authored_edges])
    authors = [a for a in results if a is not None]
    authors.sort(key=lambda a: a.position)
    return authors


async def _get_paper_summary(
    client: GvecdbClient, node_id: int, score: float = 0.0
) -> PaperSummary:
    props = await client.get_node_props(node_id)
    data = GvecdbClient.decode_paper_props(props)
    authors = await _get_paper_authors(client, node_id)
    return PaperSummary(
        node_id=node_id,
        arxiv_id=data["arxiv_id"],
        title=data["title"],
        authors=[a.name for a in authors],
        year=data["year"],
        categories=data["categories"],
        score=score,
        **_paper_summary_fields(data),
    )


async def _fetch_paper_ref(client: GvecdbClient, node_id: int) -> PaperRef | None:
    try:
        props = await client.get_node_props(node_id)
        data = GvecdbClient.decode_paper_props(props)
        return PaperRef(
            node_id=node_id,
            arxiv_id=data["arxiv_id"],
            title=data["title"],
            year=data["year"],
        )
    except RuntimeError:
        logger.warning("failed to fetch paper ref for node %d", node_id)
        return None


def _apply_filters(
    papers: list[PaperSummary],
    year_min: int | None,
    year_max: int | None,
    category: str | None,
    published_only: bool,
) -> list[PaperSummary]:
    result = papers
    if year_min is not None:
        result = [p for p in result if p.year >= year_min]
    if year_max is not None:
        result = [p for p in result if p.year <= year_max]
    if category:
        cats = set(category.split(","))
        result = [p for p in result if set(p.categories.split()) & cats]
    if published_only:
        result = [p for p in result if p.doi]
    return result


@app.get("/api/search", response_model=SearchResponse)
async def search(
    q: str,
    k: int = Query(default=20, ge=1, le=100),
    tag: str = Query(default="abstract_embedding"),
    year_min: int | None = Query(default=None),
    year_max: int | None = Query(default=None),
    category: str | None = Query(default=None),
    published_only: bool = Query(default=False),
) -> SearchResponse:
    _validate_tag(tag)
    client = await get_client()
    query_vec = await asyncio.to_thread(embed_to_bytes, q)
    # overfetch when filtering so we still return k results after post-filter
    fetch_k = k * 3 if (year_min or year_max or category or published_only) else k
    results = await client.knn_hnsw(
        vector_tag=tag,
        query=query_vec,
        k=min(fetch_k, 100),
        ef=max(fetch_k * 4, 64),
        metric=METRIC_COSINE,
    )

    async def _fetch_result(r: KnnResult) -> PaperSummary | None:
        try:
            return await _get_paper_summary(client, r.owner_id, score=1.0 - r.distance)
        except RuntimeError:
            logger.warning("failed to fetch paper for knn result owner %d", r.owner_id)
            return None

    summaries = await asyncio.gather(*[_fetch_result(r) for r in results])
    papers = [p for p in summaries if p is not None]
    papers = _apply_filters(papers, year_min, year_max, category, published_only)

    return SearchResponse(query=q, tag=tag, results=papers[:k])


@app.get("/api/paper/by-node/{node_id}", response_model=PaperDetail)
async def get_paper_by_node(node_id: int) -> PaperDetail:
    client = await get_client()

    try:
        props = await client.get_node_props(node_id)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    data = GvecdbClient.decode_paper_props(props)

    authors, outbound, inbound = await asyncio.gather(
        _get_paper_authors(client, node_id),
        client.get_outbound_edges(node_id),
        client.get_inbound_edges(node_id),
    )

    cite_edges = [e for e in outbound if e.edge_type == "cites"]
    cited_by_edges = [e for e in inbound if e.edge_type == "cites"]

    all_refs = await asyncio.gather(
        *[_fetch_paper_ref(client, e.dst) for e in cite_edges],
        *[_fetch_paper_ref(client, e.src) for e in cited_by_edges],
    )

    n_cite = len(cite_edges)
    cites = [r for r in all_refs[:n_cite] if r is not None]
    cited_by = [r for r in all_refs[n_cite:] if r is not None]

    return PaperDetail(
        node_id=node_id,
        arxiv_id=data["arxiv_id"],
        title=data["title"],
        abstract=data["abstract"],
        authors=authors,
        year=data["year"],
        categories=data["categories"],
        cites=cites,
        cited_by=cited_by,
        doi=data["doi"],
        journal_ref=data["journal_ref"],
        submitted_date=data["submitted_date"],
        page_count=data["page_count"],
        figure_count=data["figure_count"],
        version_count=data["version_count"],
        comments=data["comments"],
    )


@app.get("/api/author/{node_id}", response_model=AuthorDetail)
async def get_author(node_id: int) -> AuthorDetail:
    client = await get_client()

    try:
        props = await client.get_node_props(node_id)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    data = GvecdbClient.decode_author_props(props)
    outbound = await client.get_outbound_edges(node_id)
    authored_edges = [e for e in outbound if e.edge_type == "authored"]

    results = await asyncio.gather(
        *[_fetch_paper_ref(client, e.dst) for e in authored_edges]
    )
    papers = [p for p in results if p is not None]

    return AuthorDetail(
        node_id=node_id,
        name=data["name"],
        paper_count=data["paper_count"],
        papers=papers,
    )


@app.get("/api/graph/neighbourhood/{node_id}", response_model=GraphData)
async def get_neighbourhood(
    node_id: int,
    depth: int = Query(default=1, ge=1, le=3),
) -> GraphData:
    client = await get_client()

    visited_nodes: dict[int, GraphNode] = {}
    visited_edges: dict[int, GraphEdge] = {}
    queue: deque[tuple[int, int]] = deque([(node_id, 0)])
    seen: set[int] = {node_id}

    while queue:
        if len(visited_nodes) >= MAX_NEIGHBOURHOOD_NODES:
            break

        current_id, current_depth = queue.popleft()

        if current_id not in visited_nodes:
            try:
                info = await client.get_node_info(current_id)
                props = await client.get_node_props(current_id)
                if info.node_type == "paper":
                    data = GvecdbClient.decode_paper_props(props)
                    visited_nodes[current_id] = GraphNode(
                        id=current_id,
                        type="paper",
                        label=data["title"][:60],
                        metadata={
                            "year": data["year"],
                            "arxiv_id": data["arxiv_id"],
                            "categories": data["categories"],
                        },
                    )
                elif info.node_type == "author":
                    data = GvecdbClient.decode_author_props(props)
                    visited_nodes[current_id] = GraphNode(
                        id=current_id,
                        type="author",
                        label=data["name"],
                        metadata={"paper_count": data["paper_count"]},
                    )
            except RuntimeError:
                logger.warning("failed to fetch node %d in neighbourhood", current_id)
                continue

        if current_depth >= depth:
            continue

        try:
            outbound, inbound = await asyncio.gather(
                client.get_outbound_edges(current_id),
                client.get_inbound_edges(current_id),
            )
        except RuntimeError:
            logger.warning("failed to fetch edges for node %d", current_id)
            continue

        for edge in outbound + inbound:
            if edge.id not in visited_edges:
                visited_edges[edge.id] = GraphEdge(
                    id=edge.id, source=edge.src, target=edge.dst, type=edge.edge_type
                )

            neighbour = edge.dst if edge.src == current_id else edge.src
            if neighbour not in seen:
                seen.add(neighbour)
                queue.append((neighbour, current_depth + 1))

    all_node_ids = set(visited_nodes.keys())
    filtered_edges = [
        e for e in visited_edges.values() if e.source in all_node_ids and e.target in all_node_ids
    ]

    return GraphData(nodes=list(visited_nodes.values()), edges=filtered_edges)


@app.get("/api/similar/{node_id}", response_model=list[PaperSummary])
async def get_similar(
    node_id: int,
    k: int = Query(default=10, ge=1, le=50),
    tag: str = Query(default="abstract_embedding"),
) -> list[PaperSummary]:
    _validate_tag(tag)
    client = await get_client()

    props = await client.get_node_props(node_id)
    data = GvecdbClient.decode_paper_props(props)

    text = data["abstract"] if tag == "abstract_embedding" else data["title"]
    query_vec = await asyncio.to_thread(embed_to_bytes, text)

    results = await client.knn_hnsw(
        vector_tag=tag, query=query_vec, k=k + 1, ef=max(k * 4, 64), metric=METRIC_COSINE
    )

    filtered = [r for r in results if r.owner_id != node_id][:k]

    async def _fetch_similar(r: KnnResult) -> PaperSummary | None:
        try:
            return await _get_paper_summary(client, r.owner_id, score=1.0 - r.distance)
        except RuntimeError:
            logger.warning("failed to fetch similar paper %d", r.owner_id)
            return None

    summaries = await asyncio.gather(*[_fetch_similar(r) for r in filtered])
    return [p for p in summaries if p is not None]
