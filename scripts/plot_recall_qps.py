#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "matplotlib",
#     "numpy",
# ]
# ///
"""Plot recall@k vs QPS from benchmark JSON files.

Usage:
    uv run scripts/plot_recall_qps.py bench_results/ann_*.json bench_results/hnswlib_*.json
    uv run scripts/plot_recall_qps.py --output recall_vs_qps.pdf bench_results/*.json

Produces recall_vs_qps.pdf with Pareto curves for each implementation/config.
Groups results by k value into separate subplots when multiple k values exist.
"""

import argparse
import json
import sys
from collections import defaultdict

import matplotlib.pyplot as plt


def load_result(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def label_for(data: dict) -> str:
    impl = data.get("implementation", "gvecdb")
    m = data["params"].get("hnsw_params", {}).get("m", "?")
    return f"{impl} (m={m})"


def main():
    parser = argparse.ArgumentParser(description="Plot recall@k vs QPS")
    parser.add_argument("files", nargs="+", help="JSON result files")
    parser.add_argument(
        "--output", type=str, default="recall_vs_qps.pdf", help="Output filename"
    )
    args = parser.parse_args()

    by_k = defaultdict(list)

    for path in args.files:
        try:
            data = load_result(path)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Skipping {path}: {e}", file=sys.stderr)
            continue

        if "results" not in data:
            print(f"Skipping {path}: no 'results' key", file=sys.stderr)
            continue

        k = data["params"].get("k", 10)
        by_k[k].append((path, data))

    k_values = sorted(by_k.keys())
    n_plots = len(k_values)

    if n_plots == 0:
        print("No valid result files found.", file=sys.stderr)
        sys.exit(1)

    fig, axes = plt.subplots(1, n_plots, figsize=(8 * n_plots, 5), squeeze=False)

    markers = ["o", "s", "^", "D", "v", "<", ">", "p"]
    colors = plt.cm.tab10.colors

    for col, k in enumerate(k_values):
        ax = axes[0][col]
        entries = by_k[k]

        seen_labels = {}
        for _path, data in entries:
            recalls = [r["mean_recall"] for r in data["results"]]
            qps = [r["qps"] for r in data["results"]]
            lbl = label_for(data)

            first = lbl not in seen_labels
            if first:
                seen_labels[lbl] = len(seen_labels)
            idx = seen_labels[lbl]
            marker = markers[idx % len(markers)]
            color = colors[idx % len(colors)]

            ax.plot(
                recalls, qps, f"{marker}-",
                label=lbl if first else None,
                markersize=6, color=color,
            )

            for r in data["results"]:
                ax.annotate(
                    f'ef={r["ef"]}',
                    (r["mean_recall"], r["qps"]),
                    textcoords="offset points",
                    xytext=(5, 5),
                    fontsize=6,
                    alpha=0.7,
                )

        metric = entries[0][1]["params"].get("metric", "?") if entries else "?"
        ax.set_xlabel(f"Recall@{k}")
        ax.set_ylabel("Queries per second")
        ax.set_title(f"Recall@{k} vs QPS ({metric})")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.set_xlim(0, 1.05)

    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    print(f"Plot saved to {args.output}")


if __name__ == "__main__":
    main()
