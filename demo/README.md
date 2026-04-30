# arXiv Explorer

full-stack demo for gvecdb, ingests arXiv papers as a graph with vector embeddings, provides a web UI for semantic search and interactive graph exploration

## Architecture

```
[React/TypeScript frontend] <--REST--> [FastAPI backend] <--Cap'n Proto RPC--> [gvecdb-server]
```

## Data model

**Nodes:** `paper` (title, abstract, year, arxiv_id, categories, doi, journal_ref, submitted_date, page_count, figure_count, version_count, comments), `author` (name, paper_count)

**Edges:** `authored` (author -> paper, with position), `cites` (paper -> paper)

**Vectors:** `abstract_embedding` and `title_embedding` (384-dim, all-MiniLM-L6-v2)

## Setup

### Prerequisites

- OCaml with opam
- Python 3.12+ with [uv](https://github.com/astral-sh/uv)
- [Bun](https://bun.sh)
- arXiv metadata snapshot (JSON lines from [Kaggle](https://www.kaggle.com/datasets/Cornell-University/arxiv))

### 1. Start gvecdb-server

```bash
dune build
dune exec server/main.exe -- --db demo/data/arxiv.db \
  --capnp-listen-address unix:/tmp/gvecdb.sock \
  --capnp-secret-key-file "" --capnp-disable-tls
```

### 2. Ingest data

```bash
cd demo/backend
uv sync
uv run python -m arxiv_demo.ingest \
  --arxiv-json ~/data/arxiv-metadata-oai-snapshot.json \
  --limit 50000 \
  --socket /tmp/gvecdb.sock
```

### 3. Start backend

```bash
cd demo/backend
uv run uvicorn arxiv_demo.api:app --port 8000
```

### 4. Start frontend

```bash
cd demo/frontend
bun install
bun run dev
```
