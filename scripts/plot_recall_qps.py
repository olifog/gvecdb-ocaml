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

Produces recall_vs_qps.pdf with Pareto curves for each implementation
"""

import json
import sys

import matplotlib.pyplot as plt


def load_result(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def label_for(data: dict) -> str:
    impl = data.get("implementation", "gvecdb")
    p = data["params"]
    return f"{impl} n={p['n']} {p['metric']} {p['dim']}d"


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <json_files...>", file=sys.stderr)
        sys.exit(1)

    fig, ax = plt.subplots(1, 1, figsize=(8, 5))

    markers = ["o", "s", "^", "D", "v", "<", ">", "p"]

    for i, path in enumerate(sys.argv[1:]):
        try:
            data = load_result(path)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Skipping {path}: {e}", file=sys.stderr)
            continue

        if "results" not in data:
            print(f"Skipping {path}: no 'results' key", file=sys.stderr)
            continue

        recalls = [r["mean_recall"] for r in data["results"]]
        qps = [r["qps"] for r in data["results"]]
        marker = markers[i % len(markers)]

        ax.plot(recalls, qps, f"{marker}-", label=label_for(data), markersize=6)

        # Annotate ef values
        for r in data["results"]:
            ax.annotate(
                f'ef={r["ef"]}',
                (r["mean_recall"], r["qps"]),
                textcoords="offset points",
                xytext=(5, 5),
                fontsize=6,
                alpha=0.7,
            )

    ax.set_xlabel("Recall@10")
    ax.set_ylabel("Queries per second")
    ax.set_title("Recall vs QPS")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(0, 1.05)

    output = "recall_vs_qps.pdf"
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    print(f"Plot saved to {output}")
    plt.show()


if __name__ == "__main__":
    main()
