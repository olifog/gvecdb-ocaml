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

Outputs JSON in the same format as bench_ann for direct comparison.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

import numpy as np

try:
    import hnswlib
except ImportError:
    print("Error: install dependencies with 'uv run scripts/compare_hnswlib.py'", file=sys.stderr)
    sys.exit(1)


def generate_dataset(n: int, dim: int, seed: int) -> np.ndarray:
    rng = np.random.RandomState(seed)
    return rng.uniform(-1.0, 1.0, (n, dim)).astype(np.float32)


def brute_force_knn(data: np.ndarray, queries: np.ndarray, k: int, metric: str) -> np.ndarray:
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


def bench_hnswlib(
    n: int,
    dim: int,
    k: int,
    seed: int,
    ef_values: list[int],
    metric_name: str,
    n_queries: int,
) -> dict:
    space = "cosine" if metric_name == "cosine" else "l2"

    print(f"\n=== hnswlib: n={n} dim={dim} metric={metric_name} k={k} ===")

    data = generate_dataset(n, dim, seed)
    queries = generate_dataset(n_queries, dim, seed + 99)

    # Ground truth
    print("computing ground truth...", flush=True)
    gt_metric = "cosine" if space == "cosine" else "l2"
    gt = brute_force_knn(data, queries, k, gt_metric)

    # Build index (matching gvecdb defaults)
    M = 16
    ef_construction = 200

    idx = hnswlib.Index(space=space, dim=dim)
    idx.init_index(max_elements=n, M=M, ef_construction=ef_construction)

    print("building index...", flush=True)
    t0 = time.time()
    idx.add_items(data)
    build_time = time.time() - t0
    print(f"Build: {build_time:.2f}s ({n / build_time:.0f} vec/s)")

    results = []
    for ef in ef_values:
        idx.set_ef(ef)

        latencies = []
        all_labels = []
        for q in queries:
            t0 = time.time()
            labels, _distances = idx.knn_query(q.reshape(1, -1), k=k)
            t1 = time.time()
            latencies.append((t1 - t0) * 1e6)
            all_labels.append(labels[0])

        recall = compute_recall(gt, np.array(all_labels))
        lat = np.array(latencies)
        total_time_s = sum(latencies) / 1e6

        result = {
            "ef": ef,
            "mean_recall": float(recall),
            "qps": float(len(queries) / total_time_s) if total_time_s > 0 else 0,
            "mean_latency_us": float(np.mean(lat)),
            "p50_latency_us": float(np.percentile(lat, 50)),
            "p95_latency_us": float(np.percentile(lat, 95)),
            "p99_latency_us": float(np.percentile(lat, 99)),
        }
        results.append(result)
        print(f"  ef={ef}: recall={recall:.3f} qps={result['qps']:.0f}")

    return {
        "benchmark": "ann_recall_vs_qps",
        "implementation": "hnswlib",
        "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "params": {
            "n": n,
            "dim": dim,
            "metric": metric_name,
            "k": k,
            "n_queries": n_queries,
            "seed": seed,
            "hnsw_params": {"m": M, "m_max": M, "ef_construction": ef_construction},
        },
        "build": {
            "time_s": build_time,
            "vectors_per_second": n / build_time,
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
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    ef_values = [10, 20, 50, 100, 200, 400]
    metrics = ["cosine", "l2"]

    for metric in metrics:
        result = bench_hnswlib(
            n=args.n,
            dim=args.dim,
            k=args.k,
            seed=args.seed,
            ef_values=ef_values,
            metric_name=metric,
            n_queries=args.queries,
        )

        filename = os.path.join(
            args.output,
            f"hnswlib_{metric}_{args.n}_{args.dim}d_{result['timestamp']}.json",
        )
        with open(filename, "w") as f:
            json.dump(result, f, indent=2)
            f.write("\n")
        print(f"Results written to {filename}")

    print("\nDone.")


if __name__ == "__main__":
    main()
