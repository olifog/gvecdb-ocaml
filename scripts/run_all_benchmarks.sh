#!/bin/bash
set -euo pipefail

# Run all gvecdb dissertation benchmarks sequentially.
# Expects: OxCaml 5.2.0+ox, Docker, uv, built project.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

eval $(opam env --switch=5.2.0+ox)
export PATH="$HOME/.local/bin:$PATH"

echo "========================================"
echo "  gvecdb dissertation benchmarks"
echo "  $(date)"
echo "  $(hostname) — $(uname -r)"
echo "  $(ocaml -version)"
echo "========================================"

mkdir -p bench_results

# 1. Crash injection test (~10s)
echo ""
echo "======== [1/10] Crash injection test ========"
dune exec bench/bench_crash.exe

# 2. Concurrent reader isolation (~15s)
echo ""
echo "======== [2/10] Concurrent reader isolation ========"
dune exec bench/bench_concurrent.exe

# 3. Insertion throughput at 10K (~3min)
echo ""
echo "======== [3/10] Insertion throughput (10K) ========"
dune exec bench/bench_insert.exe -- --n=10000

# 4. Graph ops: embedded (~30s)
echo ""
echo "======== [4/10] Graph operations (embedded) ========"
dune exec bench/bench_graph.exe

# 5. Graph ops: gvecdb RPC (~5min)
echo ""
echo "======== [5/10] Graph operations (Cap'n Proto RPC) ========"
uv run scripts/bench_graph_rpc.py

# 6. Graph ops: Neo4j (~10min)
echo ""
echo "======== [6/10] Graph operations (Neo4j) ========"
uv run scripts/bench_neo4j.py --start-docker --stop-docker

# 7. ANN on 10K random (quick sanity check, ~2min)
echo ""
echo "======== [7/10] ANN recall/QPS (10K random, k=10) ========"
dune exec bench/bench_ann.exe -- --n=10000 --queries=100

# 8. Download SIFT1M + run gvecdb on it
echo ""
echo "======== [8/11] SIFT1M: download + gvecdb benchmark ========"
uv run scripts/download_datasets.py --dataset sift-128
dune exec bench/bench_ann.exe -- --dataset=datasets/sift-128 --queries=10000 --k-values=10,50

# 9. hnswlib on SIFT1M
echo ""
echo "======== [9/10] SIFT1M: hnswlib comparison ========"
uv run scripts/compare_hnswlib.py --dataset datasets/sift-128 --k 10,50 --queries 10000

# 10. Deletion recall characterisation
echo ""
echo "======== [10/10] Deletion recall (10K, 128d) ========"
dune exec bench/bench_deletion.exe -- --n=10000 --dim=128 --k=10 --ef=50 --queries=200


echo ""
echo "========================================"
echo "  All benchmarks complete!"
echo "  $(date)"
echo "  Results in bench_results/"
echo "========================================"
ls -la bench_results/
