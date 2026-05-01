#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "neo4j",
#     "numpy",
# ]
# ///
"""Neo4j graph operations benchmark for comparison with gvecdb.

Runs the same operations as bench_graph.ml against a Neo4j instance:
  1. Node creation (per-operation latency)
  2. Edge creation (per-operation latency, includes MATCH for endpoint lookup)
  3. 1-hop outbound adjacency query
  4. Typed 1-hop outbound adjacency query

Both gvecdb and Neo4j are measured as single-client sequential operations.
gvecdb is embedded (in-process); Neo4j is accessed via the Bolt protocol.
This architectural difference is inherent and should be noted when
presenting results.

Usage:
    uv run scripts/bench_neo4j.py --start-docker
    uv run scripts/bench_neo4j.py --uri bolt://localhost:7687 --password secret
"""

import argparse
import json
import os
import platform
import random
import subprocess
import sys
import time
from datetime import datetime

import numpy as np

try:
    from neo4j import GraphDatabase
except ImportError:
    print(
        "Error: install dependencies with 'uv run scripts/bench_neo4j.py'",
        file=sys.stderr,
    )
    sys.exit(1)

CONTAINER_NAME = "gvecdb-bench-neo4j"
DEFAULT_PASSWORD = "testpassword"
NEO4J_IMAGE = "neo4j:5-community"


def start_neo4j_docker(password: str) -> None:
    try:
        result = subprocess.run(
            ["docker", "inspect", CONTAINER_NAME],
            capture_output=True,
        )
        if result.returncode == 0:
            subprocess.run(
                ["docker", "start", CONTAINER_NAME],
                capture_output=True,
                check=True,
            )
            print(f"Started existing container {CONTAINER_NAME}")
        else:
            raise FileNotFoundError
    except (FileNotFoundError, subprocess.CalledProcessError):
        print(f"Creating Neo4j container {CONTAINER_NAME}...")
        subprocess.run(
            [
                "docker", "run", "-d",
                "--name", CONTAINER_NAME,
                "-p", "7687:7687",
                "-p", "7474:7474",
                "-e", f"NEO4J_AUTH=neo4j/{password}",
                "-e", "NEO4J_PLUGINS=[]",
                "-e", "NEO4J_server_memory_heap_initial__size=512m",
                "-e", "NEO4J_server_memory_heap_max__size=1g",
                "-e", "NEO4J_server_memory_pagecache_size=512m",
                NEO4J_IMAGE,
            ],
            check=True,
        )

    print("Waiting for Neo4j to be ready...", end="", flush=True)
    for _ in range(90):
        try:
            driver = GraphDatabase.driver(
                "bolt://localhost:7687", auth=("neo4j", password)
            )
            driver.verify_connectivity()
            driver.close()
            print(" ready.")
            return
        except Exception:
            time.sleep(1)
            print(".", end="", flush=True)
    print("\nFailed to connect to Neo4j after 90s", file=sys.stderr)
    sys.exit(1)


def stop_neo4j_docker() -> None:
    subprocess.run(["docker", "stop", CONTAINER_NAME], capture_output=True)


def clear_database(driver) -> None:
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()


def get_neo4j_version(driver) -> str:
    with driver.session() as session:
        result = session.run("CALL dbms.components() YIELD name, versions RETURN versions[0]")
        record = result.single()
        return record[0] if record else "unknown"


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


def bench_node_creation(driver, n: int) -> dict:
    """Create nodes one at a time, measuring per-operation latency.
    First 500 operations are warmup (JIT compilation, page cache)."""
    n_warmup = min(500, n // 2)
    print(f"\n--- Node creation (n={n}, warmup={n_warmup}) ---")
    clear_database(driver)

    latencies = []
    with driver.session() as session:
        for i in range(n + n_warmup):
            t0 = time.perf_counter()
            session.run("CREATE (n:Person {id: $id})", id=i).consume()
            t1 = time.perf_counter()
            if i >= n_warmup:
                latencies.append((t1 - t0) * 1e6)

    stats = compute_stats(latencies)
    print(f"  mean={stats['mean_latency_us']:.1f}us p95={stats['p95_latency_us']:.1f}us ({stats['qps']:.0f} ops/s)")
    return {"n": n, "warmup": n_warmup, "stats": stats}


def bench_edge_creation(driver, n_nodes: int, n_sample: int, seed: int) -> dict:
    """Create edges one at a time via MATCH+CREATE.
    Includes index-assisted endpoint lookup (the standard Neo4j pattern).
    First 500 operations are warmup."""
    n_warmup = min(500, n_sample // 2)
    n_total = n_sample + n_warmup
    print(f"\n--- Edge creation (nodes={n_nodes} sample={n_sample}, warmup={n_warmup}) ---")
    clear_database(driver)

    with driver.session() as session:
        session.run(
            "UNWIND range(0, $n - 1) AS i CREATE (n:Person {id: i})",
            n=n_nodes,
        ).consume()
        session.run("CREATE INDEX IF NOT EXISTS FOR (p:Person) ON (p.id)").consume()
        time.sleep(2)

    rng = random.Random(seed)
    edge_types = ["KNOWS", "FOLLOWS", "LIKES"]

    latencies = []
    with driver.session() as session:
        for i in range(n_total):
            src = rng.randint(0, n_nodes - 1)
            dst = rng.randint(0, n_nodes - 1)
            etype = rng.choice(edge_types)
            t0 = time.perf_counter()
            session.run(
                f"MATCH (a:Person {{id: $src}}), (b:Person {{id: $dst}}) "
                f"CREATE (a)-[:{etype}]->(b)",
                src=src, dst=dst,
            ).consume()
            t1 = time.perf_counter()
            if i >= n_warmup:
                latencies.append((t1 - t0) * 1e6)

    stats = compute_stats(latencies)
    print(f"  mean={stats['mean_latency_us']:.1f}us p95={stats['p95_latency_us']:.1f}us ({stats['qps']:.0f} ops/s)")
    return {"n_nodes": n_nodes, "n_edges": n_sample, "warmup": n_warmup, "stats": stats}


def setup_graph(driver, n_nodes: int, n_edges: int, seed: int) -> None:
    """Bulk-load a graph for adjacency query benchmarks (not measured)."""
    clear_database(driver)
    with driver.session() as session:
        session.run(
            "UNWIND range(0, $n - 1) AS i CREATE (n:Person {id: i})",
            n=n_nodes,
        ).consume()
        session.run("CREATE INDEX IF NOT EXISTS FOR (p:Person) ON (p.id)").consume()
        time.sleep(2)

    rng = random.Random(seed)
    edge_types = ["KNOWS", "FOLLOWS", "LIKES"]
    batch_size = 500

    edges = []
    for _ in range(n_edges):
        edges.append({
            "src": rng.randint(0, n_nodes - 1),
            "dst": rng.randint(0, n_nodes - 1),
            "type": rng.choice(edge_types),
        })

    print(f"  Loading {n_edges} edges...", end="", flush=True)
    with driver.session() as session:
        for i in range(0, n_edges, batch_size):
            batch = edges[i:i + batch_size]
            for etype in edge_types:
                typed = [e for e in batch if e["type"] == etype]
                if typed:
                    session.run(
                        f"UNWIND $edges AS e "
                        f"MATCH (a:Person {{id: e.src}}), (b:Person {{id: e.dst}}) "
                        f"CREATE (a)-[:{etype}]->(b)",
                        edges=[{"src": e["src"], "dst": e["dst"]} for e in typed],
                    ).consume()
    print(" done.")


def bench_adjacency_queries(
    driver, n_nodes: int, n_edges: int, n_queries: int, seed: int
) -> dict:
    """1-hop adjacency queries: outbound (all types) and typed outbound.
    Graph is bulk-loaded first (not measured). 500 warmup queries for JIT."""
    n_warmup = min(500, n_queries)
    print(f"\n--- Adjacency queries (nodes={n_nodes} edges={n_edges} queries={n_queries}, warmup={n_warmup}) ---")

    setup_graph(driver, n_nodes, n_edges, seed)

    query_rng = random.Random(seed + 1)
    edge_types = ["KNOWS", "FOLLOWS", "LIKES"]

    # Warmup
    warmup_rng = random.Random(seed + 2)
    with driver.session() as session:
        for _ in range(n_warmup):
            nid = warmup_rng.randint(0, n_nodes - 1)
            session.run(
                "MATCH (a:Person {id: $id})-[r]->(b) RETURN b.id", id=nid
            ).consume()

    # Outbound (all types)
    outbound_lats = []
    with driver.session() as session:
        for _ in range(n_queries):
            node_id = query_rng.randint(0, n_nodes - 1)
            t0 = time.perf_counter()
            session.run(
                "MATCH (a:Person {id: $id})-[r]->(b) RETURN b.id, type(r)",
                id=node_id,
            ).consume()
            t1 = time.perf_counter()
            outbound_lats.append((t1 - t0) * 1e6)

    outbound_stats = compute_stats(outbound_lats)
    print(f"  Outbound: mean={outbound_stats['mean_latency_us']:.1f}us p95={outbound_stats['p95_latency_us']:.1f}us ({outbound_stats['qps']:.0f} ops/s)")

    # Typed outbound
    typed_lats = []
    with driver.session() as session:
        for _ in range(n_queries):
            node_id = query_rng.randint(0, n_nodes - 1)
            etype = query_rng.choice(edge_types)
            t0 = time.perf_counter()
            session.run(
                f"MATCH (a:Person {{id: $id}})-[r:{etype}]->(b) RETURN b.id",
                id=node_id,
            ).consume()
            t1 = time.perf_counter()
            typed_lats.append((t1 - t0) * 1e6)

    typed_stats = compute_stats(typed_lats)
    print(f"  Typed outbound: mean={typed_stats['mean_latency_us']:.1f}us p95={typed_stats['p95_latency_us']:.1f}us ({typed_stats['qps']:.0f} ops/s)")

    return {
        "outbound": outbound_stats,
        "typed_outbound": typed_stats,
    }


def get_system_metadata() -> dict:
    return {
        "python_version": platform.python_version(),
        "os": platform.platform(),
        "cpu": platform.processor() or "unknown",
        "hostname": platform.node(),
    }


def main():
    parser = argparse.ArgumentParser(description="Neo4j graph operations benchmark")
    parser.add_argument("--uri", type=str, default="bolt://localhost:7687")
    parser.add_argument("--user", type=str, default="neo4j")
    parser.add_argument("--password", type=str, default=DEFAULT_PASSWORD)
    parser.add_argument("--nodes", type=int, default=10000)
    parser.add_argument("--edges-per-node", type=int, default=10)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=str, default="bench_results")
    parser.add_argument(
        "--start-docker",
        action="store_true",
        help="Start a Neo4j Docker container automatically",
    )
    parser.add_argument(
        "--stop-docker",
        action="store_true",
        help="Stop the Docker container after benchmarking",
    )
    args = parser.parse_args()

    if args.start_docker:
        start_neo4j_docker(args.password)

    os.makedirs(args.output, exist_ok=True)

    print(f"Connecting to {args.uri}...")
    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    driver.verify_connectivity()
    neo4j_version = get_neo4j_version(driver)
    print(f"Neo4j version: {neo4j_version}")

    n_nodes = args.nodes
    n_edges = n_nodes * args.edges_per_node
    n_queries = args.queries
    n_edge_sample = min(n_edges, 5000)

    print(f"\n=== Neo4j graph operations benchmark ===")
    print(f"    nodes={n_nodes} edges={n_edges} queries={n_queries}")

    node_result = bench_node_creation(driver, n_nodes)
    edge_result = bench_edge_creation(driver, n_nodes, n_edge_sample, args.seed)
    adj_result = bench_adjacency_queries(
        driver, n_nodes, n_edges, n_queries, args.seed
    )

    driver.close()

    if args.stop_docker:
        stop_neo4j_docker()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = {
        "benchmark": "graph_operations",
        "implementation": "neo4j",
        "neo4j_version": neo4j_version,
        "timestamp": ts,
        "system": get_system_metadata(),
        "params": {
            "n_nodes": n_nodes,
            "edges_per_node": args.edges_per_node,
            "n_queries": n_queries,
            "seed": args.seed,
        },
        "node_creation": node_result,
        "edge_creation": edge_result,
        "adjacency_queries": adj_result,
    }

    filename = os.path.join(
        args.output, f"neo4j_graph_{n_nodes}_{ts}.json"
    )
    with open(filename, "w") as f:
        json.dump(output, f, indent=2)
        f.write("\n")
    print(f"\nResults written to {filename}")
    print("Done.")


if __name__ == "__main__":
    main()
