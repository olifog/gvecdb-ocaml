#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "hnswlib",
#     "numpy",
# ]
# ///
"""Compare gvecdb HNSW against hnswlib reference implementation.

Usage:
    uv run scripts/compare_hnswlib.py [--n 10000] [--dim 128] [--seed 42]
    uv run scripts/compare_hnswlib.py --dataset datasets/sift-128
    uv run scripts/compare_hnswlib.py --m 16,32 --ef-construction 128,256

Outputs JSON in the same format as bench_ann for direct comparison.
"""

import argparse
import json
import os
import resource
import struct
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np

try:
    import hnswlib
except ImportError:
    print(
        "Error: install dependencies with 'uv run scripts/compare_hnswlib.py'",
        file=sys.stderr,
    )
    sys.exit(1)


def generate_dataset(n: int, dim: int, seed: int) -> np.ndarray:
    rng = np.random.RandomState(seed)
    return rng.uniform(-1.0, 1.0, (n, dim)).astype(np.float32)


def load_fbin(path: str) -> np.ndarray:
    """Load vectors from binary format (.fbin)."""
    with open(path, "rb") as f:
        n, dim = struct.unpack("<ii", f.read(8))
        data = np.frombuffer(f.read(n * dim * 4), dtype=np.float32)
        return data.reshape(n, dim)


def load_ibin(path: str) -> np.ndarray:
    """Load ground truth indices from binary format (.ibin)."""
    with open(path, "rb") as f:
        n, k = struct.unpack("<ii", f.read(8))
        data = np.frombuffer(f.read(n * k * 4), dtype=np.int32)
        return data.reshape(n, k)


def brute_force_knn(
    data: np.ndarray, queries: np.ndarray, k: int, metric: str
) -> np.ndarray:
    if metric == "cosine":
        norms_d = np.linalg.norm(data, axis=1, keepdims=True)
        norms_d = np.where(norms_d == 0, 1, norms_d)
        norms_q = np.linalg.norm(queries, axis=1, keepdims=True)
        norms_q = np.where(norms_q == 0, 1, norms_q)
        data_n = data / norms_d
        queries_n = queries / norms_q
        sims = queries_n @ data_n.T
        return np.argsort(-sims, axis=1)[:, :k]
    else:  # l2
        results = []
        for q in queries:
            dists = np.sum((data - q) ** 2, axis=1)
            results.append(np.argsort(dists)[:k])
        return np.array(results)


def compute_recall(ground_truth: np.ndarray, approximate: np.ndarray) -> float:
    recalls = []
    for gt, approx in zip(ground_truth, approximate):
        gt_set = set(gt.tolist())
        matches = sum(1 for a in approx if a in gt_set)
        recalls.append(matches / len(gt))
    return float(np.mean(recalls))


def get_rss_mb() -> float:
    """Peak RSS in MB via getrusage."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_maxrss / 1024  # Linux reports KB


def get_system_metadata() -> dict:
    import platform

    return {
        "python_version": platform.python_version(),
        "os": platform.platform(),
        "cpu": platform.processor() or "unknown",
        "hostname": platform.node(),
    }


def bench_hnswlib(
    n: int,
    dim: int,
    k: int,
    seed: int,
    ef_values: list[int],
    metric_name: str,
    n_queries: int,
    M: int,
    ef_construction: int,
    data: np.ndarray | None = None,
    queries: np.ndarray | None = None,
    ground_truth: np.ndarray | None = None,
) -> dict:
    space = "cosine" if metric_name == "cosine" else "l2"

    print(
        f"\n=== hnswlib: n={n} dim={dim} metric={metric_name} k={k} M={M} ef_c={ef_construction} ==="
    )

    if data is None:
        data = generate_dataset(n, dim, seed)
    if queries is None:
        queries = generate_dataset(n_queries, dim, seed + 99)

    # Ground truth
    if ground_truth is None:
        print("computing ground truth...", flush=True)
        gt_metric = "cosine" if space == "cosine" else "l2"
        ground_truth = brute_force_knn(data, queries, k, gt_metric)

    # Build index
    rss_before = get_rss_mb()

    idx = hnswlib.Index(space=space, dim=dim)
    idx.init_index(max_elements=n, M=M, ef_construction=ef_construction)

    print("building index...", flush=True)
    t0 = time.perf_counter()
    idx.add_items(data)
    build_time = time.perf_counter() - t0

    rss_after = get_rss_mb()
    print(
        f"Build: {build_time:.2f}s ({n / build_time:.0f} vec/s), "
        f"RSS delta: {rss_after - rss_before:.1f} MB"
    )

    results = []
    for ef in ef_values:
        idx.set_ef(ef)

        # Warmup
        n_warmup = max(5, n_queries // 10)
        for i in range(n_warmup):
            idx.knn_query(queries[i % n_queries].reshape(1, -1), k=k)

        # Measure: batch 10 queries per timing call to amortize overhead
        batch_size = min(10, n_queries)
        latencies = []
        all_labels = []
        qi = 0
        while qi < n_queries:
            batch_end = min(n_queries, qi + batch_size)
            batch_queries = queries[qi:batch_end]
            actual_batch = batch_end - qi

            t0 = time.perf_counter()
            labels, _distances = idx.knn_query(batch_queries, k=k)
            t1 = time.perf_counter()

            per_query_us = (t1 - t0) * 1e6 / actual_batch
            for _ in range(actual_batch):
                latencies.append(per_query_us)
            for lbl in labels:
                all_labels.append(lbl)
            qi = batch_end

        recall = compute_recall(ground_truth[:n_queries], np.array(all_labels))
        lat = np.array(latencies)
        total_time_s = sum(latencies) / 1e6

        result = {
            "ef": ef,
            "mean_recall": float(recall),
            "qps": float(len(queries) / total_time_s) if total_time_s > 0 else 0,
            "mean_latency_us": float(np.mean(lat)),
            "stddev_us": float(np.std(lat)),
            "p50_latency_us": float(np.percentile(lat, 50)),
            "p95_latency_us": float(np.percentile(lat, 95)),
            "p99_latency_us": float(np.percentile(lat, 99)),
            "min_latency_us": float(np.min(lat)),
            "max_latency_us": float(np.max(lat)),
        }
        results.append(result)
        print(
            f"  ef={ef}: recall={recall:.3f} qps={result['qps']:.0f} "
            f"p50={result['p50_latency_us']:.0f}us p99={result['p99_latency_us']:.0f}us"
        )

    return {
        "benchmark": "ann_recall_vs_qps",
        "implementation": "hnswlib",
        "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "system": get_system_metadata(),
        "params": {
            "n": n,
            "dim": dim,
            "metric": metric_name,
            "k": k,
            "n_queries": n_queries,
            "seed": seed,
            "hnsw_params": {
                "m": M,
                "m_max": M,
                "ef_construction": ef_construction,
            },
        },
        "build": {
            "time_s": build_time,
            "vectors_per_second": n / build_time,
            "rss_before_mb": rss_before,
            "rss_after_mb": rss_after,
            "rss_delta_mb": rss_after - rss_before,
        },
        "results": results,
    }


def main():
    parser = argparse.ArgumentParser(description="hnswlib benchmark comparison")
    parser.add_argument("--n", type=int, default=10000)
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=str, default="bench_results")
    parser.add_argument(
        "--dataset",
        type=str,
        default="",
        help="Path to dataset directory (must contain base.fbin, queries.fbin)",
    )
    parser.add_argument(
        "--m",
        type=str,
        default="16",
        help="Comma-separated M values for parameter sweep (default: 16)",
    )
    parser.add_argument(
        "--ef-construction",
        type=str,
        default="200",
        help="Comma-separated ef_construction values (default: 200)",
    )
    parser.add_argument(
        "--ef-values",
        type=str,
        default="10,20,50,100,200,400",
        help="Comma-separated ef_search values",
    )
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    m_values = [int(x) for x in args.m.split(",")]
    ef_c_values = [int(x) for x in args.ef_construction.split(",")]
    ef_values = [int(x) for x in args.ef_values.split(",")]

    # Load dataset if specified
    data = None
    queries_data = None
    ground_truth = None
    actual_dim = args.dim

    if args.dataset:
        dataset_dir = Path(args.dataset)
        base_path = dataset_dir / "base.fbin"
        query_path = dataset_dir / "queries.fbin"
        gt_path = dataset_dir / "groundtruth.ibin"

        if not base_path.exists():
            print(f"Error: {base_path} not found", file=sys.stderr)
            sys.exit(1)

        print(f"Loading dataset from {dataset_dir}...")
        data = load_fbin(str(base_path))
        actual_dim = data.shape[1]
        args.n = min(args.n, data.shape[0]) if args.n < data.shape[0] else data.shape[0]
        data = data[: args.n]
        print(f"  Base vectors: {data.shape}")

        if query_path.exists():
            queries_data = load_fbin(str(query_path))
            args.queries = min(args.queries, queries_data.shape[0])
            queries_data = queries_data[: args.queries]
            print(f"  Query vectors: {queries_data.shape}")

        if gt_path.exists():
            ground_truth = load_ibin(str(gt_path))
            ground_truth = ground_truth[: args.queries, : args.k]
            print(f"  Ground truth: {ground_truth.shape}")

    metrics = ["cosine", "l2"]

    for metric in metrics:
        for M in m_values:
            for ef_c in ef_c_values:
                result = bench_hnswlib(
                    n=args.n,
                    dim=actual_dim,
                    k=args.k,
                    seed=args.seed,
                    ef_values=ef_values,
                    metric_name=metric,
                    n_queries=args.queries,
                    M=M,
                    ef_construction=ef_c,
                    data=data,
                    queries=queries_data,
                    ground_truth=ground_truth,
                )

                filename = os.path.join(
                    args.output,
                    f"hnswlib_{metric}_{args.n}_{actual_dim}d_m{M}_efc{ef_c}_{result['timestamp']}.json",
                )
                with open(filename, "w") as f:
                    json.dump(result, f, indent=2)
                    f.write("\n")
                print(f"Results written to {filename}")

    print("\nDone.")


if __name__ == "__main__":
    main()
