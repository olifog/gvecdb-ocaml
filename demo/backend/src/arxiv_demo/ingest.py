from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Coroutine
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import TypedDict

import capnp
import httpx
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn

from arxiv_demo.embeddings import embed_batch_to_bytes
from arxiv_demo.gvecdb_client import METRIC_COSINE, GvecdbClient


class ArxivRecord(TypedDict):
    id: str
    title: str
    abstract: str
    authors: list[str]
    categories: str
    year: int
    doi: str
    journal_ref: str
    submitted_date: str
    page_count: int
    figure_count: int
    version_count: int
    comments: str


console = Console()

EMBED_BATCH_SIZE = 96
RPC_CONCURRENCY = 32
MIN_YEAR = 0
REBUILD_INTERVAL = 600_000

OPENALEX_BATCH_SIZE = 50
OPENALEX_EMAIL = "demo@gvecdb.dev"

_RE_PAGES = re.compile(r"(\d+)\s*pages?", re.IGNORECASE)
_RE_FIGURES = re.compile(r"(\d+)\s*figures?", re.IGNORECASE)


def parse_authors(authors_parsed: list[list[str]]) -> list[str]:
    names: list[str] = []
    for entry in authors_parsed:
        if not entry:
            continue
        last = entry[0].strip() if len(entry) > 0 else ""
        first = entry[1].strip() if len(entry) > 1 else ""
        if last:
            name = f"{first} {last}".strip() if first else last
            names.append(name)
    return names


def parse_authors_string(authors_str: str) -> list[str]:
    return [a.strip() for a in authors_str.split(",") if a.strip()]


def extract_year(update_date: str) -> int:
    m = re.match(r"(\d{4})", update_date)
    return int(m.group(1)) if m else 0


def extract_page_count(comments: str) -> int:
    m = _RE_PAGES.search(comments)
    return min(int(m.group(1)), 65535) if m else 0


def extract_figure_count(comments: str) -> int:
    m = _RE_FIGURES.search(comments)
    return min(int(m.group(1)), 65535) if m else 0


def extract_submitted_date(versions: list[dict[str, str]]) -> str:
    if not versions:
        return ""
    created = versions[0].get("created", "")
    if not created:
        return ""
    try:
        dt = parsedate_to_datetime(created)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return ""


def load_papers(
    json_path: Path,
    limit: int,
    categories: set[str],
) -> list[ArxivRecord]:
    papers: list[ArxivRecord] = []
    console.print(f"[bold]Loading papers from {json_path}...[/bold]")

    with open(json_path) as f:
        for line in f:
            if len(papers) >= limit:
                break
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            cats = record.get("categories", "")
            cat_set = set(cats.split())
            if categories and not cat_set & categories:
                continue

            year = extract_year(record.get("update_date", ""))
            if MIN_YEAR and year < MIN_YEAR:
                continue

            if "authors_parsed" in record:
                authors = parse_authors(record["authors_parsed"])
            else:
                authors = parse_authors_string(record.get("authors", ""))

            if not authors:
                continue

            abstract = record.get("abstract", "").strip().replace("\n", " ")
            title = record.get("title", "").strip().replace("\n", " ")
            if not abstract or not title:
                continue

            comments_raw = record.get("comments", "") or ""
            versions = record.get("versions", [])

            papers.append(
                ArxivRecord(
                    id=record["id"],
                    title=title,
                    abstract=abstract,
                    authors=authors,
                    categories=cats,
                    year=year,
                    doi=record.get("doi", "") or "",
                    journal_ref=record.get("journal-ref", "") or "",
                    submitted_date=extract_submitted_date(versions),
                    page_count=extract_page_count(comments_raw),
                    figure_count=extract_figure_count(comments_raw),
                    version_count=min(len(versions), 255),
                    comments=comments_raw.strip().replace("\n", " "),
                )
            )

    console.print(f"[green]Loaded {len(papers)} papers[/green]")
    return papers


async def fetch_citations(
    arxiv_ids: list[str],
    progress: Progress,
) -> dict[str, list[str]]:
    known_arxiv = set(arxiv_ids)
    # openalex id -> arxiv id, for papers in our dataset
    oa_to_arxiv: dict[str, str] = {}
    # openalex id -> list of referenced openalex ids
    oa_refs: dict[str, list[str]] = {}

    task = progress.add_task("Fetching citations from OpenAlex", total=len(arxiv_ids))

    async with httpx.AsyncClient(
        timeout=30,
        headers={"User-Agent": f"gvecdb-demo/0.1 (mailto:{OPENALEX_EMAIL})"},
    ) as http:
        for batch_start in range(0, len(arxiv_ids), OPENALEX_BATCH_SIZE):
            batch = arxiv_ids[batch_start : batch_start + OPENALEX_BATCH_SIZE]
            doi_filter = "|".join(
                f"https://doi.org/10.48550/arXiv.{aid}" for aid in batch
            )

            try:
                resp = await http.get(
                    "https://api.openalex.org/works",
                    params={
                        "filter": f"doi:{doi_filter}",
                        "select": "id,ids,referenced_works",
                        "per_page": str(OPENALEX_BATCH_SIZE),
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            except (httpx.HTTPError, json.JSONDecodeError):
                progress.advance(task, len(batch))
                continue

            for work in data.get("results", []):
                oa_id = work.get("id", "")
                doi = work.get("ids", {}).get("doi", "")
                lower_doi = doi.lower()
                if "10.48550/arxiv." in lower_doi:
                    arxiv_id = lower_doi.split("10.48550/arxiv.")[-1]
                    if arxiv_id in known_arxiv:
                        oa_to_arxiv[oa_id] = arxiv_id

                refs = work.get("referenced_works", [])
                if refs:
                    oa_refs[oa_id] = refs

            progress.advance(task, len(batch))
            await asyncio.sleep(0.1)

    # any ref whose openalex id is already in oa_to_arxiv is a within-dataset citation
    citations: dict[str, list[str]] = {}
    for oa_id, refs in oa_refs.items():
        src_arxiv = oa_to_arxiv.get(oa_id)
        if not src_arxiv:
            continue
        targets = [
            oa_to_arxiv[ref_id]
            for ref_id in refs
            if ref_id in oa_to_arxiv and oa_to_arxiv[ref_id] != src_arxiv
        ]
        if targets:
            citations[src_arxiv] = targets

    total = sum(len(v) for v in citations.values())
    console.print(
        f"[green]Found {total} citation edges across {len(citations)} papers[/green]"
    )
    return citations


async def ingest_papers(
    client: GvecdbClient,
    papers: list[ArxivRecord],
    state_file: Path,
) -> dict[str, int]:
    ingested_ids: set[str] = set()
    author_name_to_id: dict[str, int] = {}
    arxiv_id_to_node: dict[str, int] = {}

    if state_file.exists():
        state = json.loads(state_file.read_text())
        ingested_ids = set(state.get("ingested_ids", []))
        author_name_to_id = {k: int(v) for k, v in state.get("author_map", {}).items()}
        arxiv_id_to_node = {k: int(v) for k, v in state.get("paper_map", {}).items()}
        console.print(
            f"[yellow]Resuming: {len(ingested_ids)} papers already ingested[/yellow]"
        )

    remaining = [p for p in papers if p["id"] not in ingested_ids]
    if not remaining:
        console.print("[green]All papers already ingested[/green]")
        return arxiv_id_to_node

    console.print(f"[bold]{len(remaining)} papers to ingest[/bold]")

    author_paper_count: dict[str, int] = {}
    for p in remaining:
        for a in p["authors"]:
            author_paper_count[a] = author_paper_count.get(a, 0) + 1

    new_authors = [name for name in author_paper_count if name not in author_name_to_id]

    progress = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    )

    def _save_state() -> None:
        state_file.write_text(
            json.dumps({
                "ingested_ids": list(ingested_ids),
                "author_map": author_name_to_id,
                "paper_map": arxiv_id_to_node,
            })
        )

    with progress:
        if new_authors:
            task_authors = progress.add_task("Creating authors", total=len(new_authors))

            async def _create_author(name: str) -> tuple[str, int]:
                node_id = await client.create_node("author")
                props = GvecdbClient.build_author_props(name, author_paper_count[name])
                await client.set_node_props(node_id, "author", props)
                return name, node_id

            author_coros = [_create_author(name) for name in new_authors]
            sem = asyncio.Semaphore(RPC_CONCURRENCY)

            async def _bounded_author(
                coro: Coroutine[None, None, tuple[str, int]],
            ) -> tuple[str, int]:
                async with sem:
                    result = await coro
                    progress.advance(task_authors)
                    return result

            author_results = await asyncio.gather(
                *[_bounded_author(c) for c in author_coros]
            )
            for name, node_id in author_results:
                author_name_to_id[name] = node_id

        task_papers = progress.add_task("Ingesting papers", total=len(remaining))
        since_rebuild = 0

        for batch_start in range(0, len(remaining), EMBED_BATCH_SIZE):
            batch = remaining[batch_start : batch_start + EMBED_BATCH_SIZE]
            sem = asyncio.Semaphore(RPC_CONCURRENCY)

            async def _create_paper(p: ArxivRecord) -> tuple[str, int]:
                async with sem:
                    node_id = await client.create_node("paper")
                    props = GvecdbClient.build_paper_props(
                        title=p["title"],
                        abstract=p["abstract"],
                        year=p["year"],
                        arxiv_id=p["id"],
                        categories=p["categories"],
                        doi=p["doi"],
                        journal_ref=p["journal_ref"],
                        submitted_date=p["submitted_date"],
                        page_count=p["page_count"],
                        figure_count=p["figure_count"],
                        version_count=p["version_count"],
                        comments=p["comments"],
                    )
                    await client.set_node_props(node_id, "paper", props)
                    return p["id"], node_id

            paper_results = await asyncio.gather(*[_create_paper(p) for p in batch])
            for arxiv_id, node_id in paper_results:
                arxiv_id_to_node[arxiv_id] = node_id

            edge_coros = []
            for p in batch:
                paper_node = arxiv_id_to_node[p["id"]]
                for pos, author_name in enumerate(p["authors"]):
                    author_node = author_name_to_id.get(author_name)
                    if author_node is None:
                        continue

                    async def _create_edge(
                        a_node: int = author_node,
                        p_node: int = paper_node,
                        position: int = pos,
                    ) -> None:
                        async with sem:
                            edge_id = await client.create_edge("authored", a_node, p_node)
                            props = GvecdbClient.build_authored_props(min(position, 255))
                            await client.set_edge_props(edge_id, "authored", props)

                    edge_coros.append(_create_edge())

            if edge_coros:
                await asyncio.gather(*edge_coros)

            abstracts = [p["abstract"] for p in batch]
            titles = [p["title"] for p in batch]
            abstract_vecs = embed_batch_to_bytes(abstracts)
            title_vecs = embed_batch_to_bytes(titles)

            vec_coros = []
            for j, p in enumerate(batch):
                paper_node = arxiv_id_to_node[p["id"]]

                async def _store_vecs(
                    node: int = paper_node,
                    abs_vec: bytes = abstract_vecs[j],
                    title_vec: bytes = title_vecs[j],
                ) -> None:
                    async with sem:
                        await client.create_vector(
                            node, "abstract_embedding", abs_vec, metric=METRIC_COSINE
                        )
                        await client.create_vector(
                            node, "title_embedding", title_vec, metric=METRIC_COSINE
                        )

                vec_coros.append(_store_vecs())

            await asyncio.gather(*vec_coros)

            for p in batch:
                ingested_ids.add(p["id"])
                progress.advance(task_papers)
            since_rebuild += len(batch)
            _save_state()

            if since_rebuild >= REBUILD_INTERVAL:
                console.print(
                    f"[yellow]Rebuilding HNSW indexes ({len(ingested_ids)} papers)...[/yellow]"
                )
                await client.rebuild_hnsw_index("abstract_embedding")
                await client.rebuild_hnsw_index("title_embedding")
                since_rebuild = 0
                console.print("[green]Rebuild complete[/green]")

    console.print(f"  Papers: {len(arxiv_id_to_node)}")
    console.print(f"  Authors: {len(author_name_to_id)}")
    return arxiv_id_to_node


async def ingest_citations(
    client: GvecdbClient,
    arxiv_id_to_node: dict[str, int],
    progress: Progress,
) -> int:
    citations = await fetch_citations(list(arxiv_id_to_node.keys()), progress)

    total_cites = sum(len(v) for v in citations.values())
    if total_cites == 0:
        return 0

    cite_task = progress.add_task("Creating citation edges", total=total_cites)
    sem = asyncio.Semaphore(RPC_CONCURRENCY)

    cite_coros = []
    for src_arxiv, dst_arxivs in citations.items():
        src_node = arxiv_id_to_node.get(src_arxiv)
        if src_node is None:
            continue
        for dst_arxiv in dst_arxivs:
            dst_node = arxiv_id_to_node.get(dst_arxiv)
            if dst_node is None:
                continue

            async def _create_cite(s: int = src_node, d: int = dst_node) -> None:
                async with sem:
                    edge_id = await client.create_edge("cites", s, d)
                    props = GvecdbClient.build_cites_props()
                    await client.set_edge_props(edge_id, "cites", props)
                    progress.advance(cite_task)

            cite_coros.append(_create_cite())

    if cite_coros:
        await asyncio.gather(*cite_coros)

    return len(cite_coros)


async def run(
    socket_path: str,
    papers: list[ArxivRecord],
    state_file: Path,
    skip_citations: bool = False,
    citations_only: bool = False,
) -> None:
    client = await GvecdbClient.connect(socket_path)

    console.print("[bold]Registering schemas...[/bold]")
    await client.register_schemas()
    console.print("[green]Schemas registered[/green]")

    if citations_only:
        if not state_file.exists():
            console.print("[red]No state file found — run full ingest first[/red]")
            return
        state = json.loads(state_file.read_text())
        arxiv_id_to_node = {k: int(v) for k, v in state.get("paper_map", {}).items()}
        console.print(f"[bold]Loading {len(arxiv_id_to_node)} papers from state[/bold]")
    else:
        arxiv_id_to_node = await ingest_papers(client, papers, state_file)

    if not skip_citations and arxiv_id_to_node:
        progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        )
        with progress:
            n = await ingest_citations(client, arxiv_id_to_node, progress)
        console.print(f"  Citation edges: {n}")

    console.print(f"\n[bold green]Done![/bold green]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest arXiv data into gvecdb")
    parser.add_argument("--arxiv-json", type=Path, required=True)
    parser.add_argument("--socket", default="/tmp/gvecdb.sock")
    parser.add_argument("--limit", type=int, default=3000000)
    parser.add_argument("--categories", default="")
    parser.add_argument("--state-file", type=Path, default=Path("ingest_state.json"))
    parser.add_argument("--skip-citations", action="store_true")
    parser.add_argument(
        "--citations-only", action="store_true",
        help="skip paper ingest, only fetch citations (requires prior ingest state)",
    )
    args = parser.parse_args()

    categories = set(args.categories.split(",")) if args.categories else set()
    papers = load_papers(args.arxiv_json, args.limit, categories) if not args.citations_only else []

    if not papers and not args.citations_only:
        console.print("[red]No papers found matching criteria[/red]")
        sys.exit(1)

    asyncio.run(  # type: ignore[no-untyped-call]
        capnp.run(run(args.socket, papers, args.state_file, args.skip_citations, args.citations_only))
    )


if __name__ == "__main__":
    main()
