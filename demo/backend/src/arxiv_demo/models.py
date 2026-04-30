from __future__ import annotations

from pydantic import BaseModel


class PaperSummary(BaseModel):
    node_id: int
    arxiv_id: str
    title: str
    authors: list[str]
    year: int
    categories: str
    score: float = 0.0
    doi: str = ""
    journal_ref: str = ""
    submitted_date: str = ""
    page_count: int = 0
    figure_count: int = 0
    version_count: int = 0


class PaperDetail(BaseModel):
    node_id: int
    arxiv_id: str
    title: str
    abstract: str
    authors: list[AuthorRef]
    year: int
    categories: str
    cites: list[PaperRef]
    cited_by: list[PaperRef]
    doi: str = ""
    journal_ref: str = ""
    submitted_date: str = ""
    page_count: int = 0
    figure_count: int = 0
    version_count: int = 0
    comments: str = ""


class AuthorRef(BaseModel):
    node_id: int
    name: str
    position: int = 0


class PaperRef(BaseModel):
    node_id: int
    arxiv_id: str
    title: str
    year: int = 0


class AuthorDetail(BaseModel):
    node_id: int
    name: str
    paper_count: int
    papers: list[PaperRef]


class GraphNode(BaseModel):
    id: int
    type: str
    label: str
    metadata: dict[str, str | int | float] = {}


class GraphEdge(BaseModel):
    id: int
    source: int
    target: int
    type: str


class GraphData(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]


class SearchResponse(BaseModel):
    query: str
    tag: str
    results: list[PaperSummary]


class PaperStats(BaseModel):
    node_id: int
    citation_count: int
    cited_by_count: int
    author_count: int
    categories: list[str]


class DiscoveryResult(BaseModel):
    cited: list[PaperSummary]
    undiscovered: list[PaperSummary]


PaperDetail.model_rebuild()
