#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pycapnp",
#     "numpy",
# ]
# ///
"""gvecdb graph operations benchmark over Cap'n Proto RPC.

Runs the same 4 operations as bench_graph.ml but over the RPC interface,
for a fair server-vs-server comparison with Neo4j.

Starts a gvecdb-server child process, runs benchmarks via Cap'n Proto
over a Unix domain socket, then stops the server.

Usage:
    uv run scripts/bench_graph_rpc.py
    uv run scripts/bench_graph_rpc.py --nodes=10000 --edges-per-node=10
"""

import argparse
import asyncio
import json
import os
import platform
import random
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import numpy as np

try:
    import capnp
except ImportError:
    print(
        "Error: install dependencies with 'uv run scripts/bench_graph_rpc.py'",
        file=sys.stderr,
    )
    sys.exit(1)

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "server" / "gvecdb_api.capnp"
gvecdb_api = capnp.load(str(SCHEMA_PATH))


def compute_stats(latencies_us: list[float]) -> dict:
    lat = np.array(latencies_us)
    total = float(np.sum(lat))
    n = len(lat)
    return {
        "mean_latency_us": float(np.mean(lat)),
        "stddev_us": float(np.std(lat)),
        "p50_latency_us": float(np.percentile(lat, 50)),
        "p95_latency_us": float(np.percentile(lat, 95)),
        "p99_latency_us": float(np.percentile(lat, 99)),
        "min_latency_us": float(np.min(lat)),
        "max_latency_us": float(np.max(lat)),
        "qps": float(n / (total / 1e6)) if total > 0 else 0,
        "count": n,
    }


def find_server_binary() -> str:
    project_dir = Path(__file__).resolve().parent.parent
    candidates = [
        project_dir / "_build" / "default" / "server" / "main.exe",
        project_dir / "_build" / "install" / "default" / "bin" / "gvecdb-server",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    raise RuntimeError(
        "gvecdb-server binary not found — run 'dune build' first"
    )


def start_server(db_path: str, socket_path: str) -> subprocess.Popen:
    binary = find_server_binary()
    cmd = [
        binary,
        f"--db={db_path}",
        f"--capnp-listen-address=unix:{socket_path}",
        "--capnp-secret-key-file=",
        "--capnp-disable-tls",
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for _ in range(30):
        if os.path.exists(socket_path):
            time.sleep(0.2)
            return proc
        time.sleep(0.2)
    stderr = proc.stderr.read().decode() if proc.stderr else ""
    proc.kill()
    raise RuntimeError(f"gvecdb server failed to start: {stderr[:500]}")


def stop_server(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


async def connect(socket_path: str):
    conn = await capnp.AsyncIoStream.create_unix_connection(path=socket_path)
    tpc = capnp.TwoPartyClient(conn)
    client = tpc.bootstrap().cast_as(gvecdb_api.Gvecdb)
    return client, conn


async def bench_node_creation(client, n: int, n_warmup: int) -> dict:
    print(f"\n--- Node creation (n={n}, warmup={n_warmup}) ---")
    latencies = []
    for i in range(n + n_warmup):
        t0 = time.perf_counter()
        result = await client.createNode(nodeType="person")
        t1 = time.perf_counter()
        err = result.error
        if err:
            raise RuntimeError(f"createNode failed: {err}")
        if i >= n_warmup:
            latencies.append((t1 - t0) * 1e6)

    stats = compute_stats(latencies)
    print(f"  mean={stats['mean_latency_us']:.1f}us p95={stats['p95_latency_us']:.1f}us ({stats['qps']:.0f} ops/s)")
    return {"n": n, "warmup": n_warmup, "stats": stats}


async def bench_edge_creation(
    client, n_nodes: int, n_sample: int, n_warmup: int, seed: int
) -> dict:
    print(f"\n--- Edge creation (sample={n_sample}, warmup={n_warmup}) ---")

    # Bulk-create nodes (not measured)
    node_ids = []
    for _ in range(n_nodes):
        result = await client.createNode(nodeType="person")
        node_ids.append(int(result.nodeId))

    rng = random.Random(seed)
    edge_types = ["knows", "follows", "likes"]

    latencies = []
    n_total = n_sample + n_warmup
    for i in range(n_total):
        src = node_ids[rng.randint(0, n_nodes - 1)]
        dst = node_ids[rng.randint(0, n_nodes - 1)]
        etype = rng.choice(edge_types)
        t0 = time.perf_counter()
        result = await client.createEdge(edgeType=etype, src=src, dst=dst)
        t1 = time.perf_counter()
        err = result.error
        if err:
            raise RuntimeError(f"createEdge failed: {err}")
        if i >= n_warmup:
            latencies.append((t1 - t0) * 1e6)

    stats = compute_stats(latencies)
    print(f"  mean={stats['mean_latency_us']:.1f}us p95={stats['p95_latency_us']:.1f}us ({stats['qps']:.0f} ops/s)")
    return {"n_nodes": n_nodes, "n_edges": n_sample, "warmup": n_warmup, "stats": stats}


async def bench_adjacency_queries(
    client, n_nodes: int, n_edges: int, n_queries: int, n_warmup: int, seed: int
) -> dict:
    print(f"\n--- Adjacency queries (nodes={n_nodes} edges={n_edges} queries={n_queries}, warmup={n_warmup}) ---")

    # Bulk-create graph (not measured)
    node_ids = []
    for _ in range(n_nodes):
        result = await client.createNode(nodeType="person")
        node_ids.append(int(result.nodeId))

    rng = random.Random(seed)
    edge_types = ["knows", "follows", "likes"]
    for _ in range(n_edges):
        src = node_ids[rng.randint(0, n_nodes - 1)]
        dst = node_ids[rng.randint(0, n_nodes - 1)]
        etype = rng.choice(edge_types)
        await client.createEdge(edgeType=etype, src=src, dst=dst)

    query_rng = random.Random(seed + 1)

    # Warmup
    warmup_rng = random.Random(seed + 2)
    for _ in range(n_warmup):
        nid = node_ids[warmup_rng.randint(0, n_nodes - 1)]
        await client.getOutboundEdges(nodeId=nid)

    # Outbound (all types)
    outbound_lats = []
    for _ in range(n_queries):
        nid = node_ids[query_rng.randint(0, n_nodes - 1)]
        t0 = time.perf_counter()
        await client.getOutboundEdges(nodeId=nid)
        t1 = time.perf_counter()
        outbound_lats.append((t1 - t0) * 1e6)

    outbound_stats = compute_stats(outbound_lats)
    print(f"  Outbound: mean={outbound_stats['mean_latency_us']:.1f}us p95={outbound_stats['p95_latency_us']:.1f}us ({outbound_stats['qps']:.0f} ops/s)")

    # Typed outbound
    typed_lats = []
    for _ in range(n_queries):
        nid = node_ids[query_rng.randint(0, n_nodes - 1)]
        etype = query_rng.choice(edge_types)
        t0 = time.perf_counter()
        await client.getOutboundEdgesFiltered(nodeId=nid, edgeType=etype, filters=[])
        t1 = time.perf_counter()
        typed_lats.append((t1 - t0) * 1e6)

    typed_stats = compute_stats(typed_lats)
    print(f"  Typed outbound: mean={typed_stats['mean_latency_us']:.1f}us p95={typed_stats['p95_latency_us']:.1f}us ({typed_stats['qps']:.0f} ops/s)")

    return {
        "outbound": outbound_stats,
        "typed_outbound": typed_stats,
    }


async def run_benchmarks(socket_path: str, args) -> dict:
    client, conn = await connect(socket_path)

    n_nodes = args.nodes
    n_edges = n_nodes * args.edges_per_node
    n_queries = args.queries
    n_warmup = min(500, n_nodes // 2)
    n_edge_sample = min(n_edges, 5000)

    print(f"\n=== gvecdb RPC graph operations benchmark ===")
    print(f"    nodes={n_nodes} edges={n_edges} queries={n_queries}")

    node_result = await bench_node_creation(client, n_nodes, n_warmup)
    edge_result = await bench_edge_creation(
        client, n_nodes, n_edge_sample, n_warmup, args.seed
    )
    adj_result = await bench_adjacency_queries(
        client, n_nodes, n_edges, n_queries, n_warmup, args.seed
    )

    return {
        "node_creation": node_result,
        "edge_creation": edge_result,
        "adjacency_queries": adj_result,
    }


def get_system_metadata() -> dict:
    return {
        "python_version": platform.python_version(),
        "os": platform.platform(),
        "cpu": platform.processor() or "unknown",
        "hostname": platform.node(),
    }


def main():
    parser = argparse.ArgumentParser(
        description="gvecdb graph benchmark over Cap'n Proto RPC"
    )
    parser.add_argument("--nodes", type=int, default=10000)
    parser.add_argument("--edges-per-node", type=int, default=10)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=str, default="bench_results")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    tmpdir = tempfile.mkdtemp(prefix="gvecdb_bench_rpc_")
    db_path = os.path.join(tmpdir, "bench.db")
    socket_path = os.path.join(tmpdir, "bench.sock")

    print(f"Starting gvecdb server (db={db_path})...")
    proc = start_server(db_path, socket_path)

    try:
        results = asyncio.run(capnp.run(run_benchmarks(socket_path, args)))
    finally:
        print("\nStopping server...")
        stop_server(proc)
        # Cleanup
        for f in os.listdir(tmpdir):
            try:
                p = os.path.join(tmpdir, f)
                if os.path.isdir(p):
                    import shutil
                    shutil.rmtree(p)
                else:
                    os.remove(p)
            except OSError:
                pass
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = {
        "benchmark": "graph_operations",
        "implementation": "gvecdb_rpc",
        "timestamp": ts,
        "system": get_system_metadata(),
        "params": {
            "n_nodes": args.nodes,
            "edges_per_node": args.edges_per_node,
            "n_queries": args.queries,
            "seed": args.seed,
            "transport": "capnp unix socket",
        },
        **results,
    }

    filename = os.path.join(
        args.output, f"gvecdb_rpc_graph_{args.nodes}_{ts}.json"
    )
    with open(filename, "w") as f:
        json.dump(output, f, indent=2)
        f.write("\n")
    print(f"\nResults written to {filename}")
    print("Done.")


if __name__ == "__main__":
    main()
