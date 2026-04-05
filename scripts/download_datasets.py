#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "h5py",
#     "numpy",
# ]
# ///
"""Download and convert standard ANN benchmark datasets.

Downloads HDF5 files from ann-benchmarks.com and converts them to a simple
binary format that the OCaml benchmark suite can load directly.

Binary format (.fbin):
  - 4 bytes: int32_le n (number of vectors)
  - 4 bytes: int32_le dim (dimension)
  - n * dim * 4 bytes: float32_le row-major data

Ground truth format (.ibin):
  - 4 bytes: int32_le n_queries
  - 4 bytes: int32_le k
  - n_queries * k * 4 bytes: int32_le row-major indices

Usage:
    uv run scripts/download_datasets.py [--dataset sift-128] [--output datasets/]
    uv run scripts/download_datasets.py --list
    uv run scripts/download_datasets.py --all
"""

import argparse
import os
import struct
import sys
import urllib.request

import h5py
import numpy as np

DATASETS = {
    "sift-128": {
        "url": "http://ann-benchmarks.com/sift-128-euclidean.hdf5",
        "metric": "euclidean",
        "dim": 128,
        "description": "SIFT 1M (128d, euclidean) — the universal ANN baseline",
    },
    "glove-100": {
        "url": "http://ann-benchmarks.com/glove-100-angular.hdf5",
        "metric": "angular",
        "dim": 100,
        "description": "GloVe 1.2M (100d, angular/cosine) — word embeddings",
    },
    "gist-960": {
        "url": "http://ann-benchmarks.com/gist-960-euclidean.hdf5",
        "metric": "euclidean",
        "dim": 960,
        "description": "GIST 1M (960d, euclidean) — high-dimensional stress test",
    },
    "fashion-mnist-784": {
        "url": "http://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5",
        "metric": "euclidean",
        "dim": 784,
        "description": "Fashion-MNIST 60K (784d, euclidean) — small & fast",
    },
    "nytimes-256": {
        "url": "http://ann-benchmarks.com/nytimes-256-angular.hdf5",
        "metric": "angular",
        "dim": 256,
        "description": "NYTimes 290K (256d, angular/cosine) — document embeddings",
    },
}


def download_file(url: str, dest: str) -> None:
    """Download a file with progress reporting."""
    if os.path.exists(dest):
        print(f"  Already downloaded: {dest}")
        return

    print(f"  Downloading {url}...")
    tmp = dest + ".tmp"

    def reporthook(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            pct = min(100, downloaded * 100 // total_size)
            mb = downloaded / (1024 * 1024)
            total_mb = total_size / (1024 * 1024)
            print(f"\r  {mb:.1f}/{total_mb:.1f} MB ({pct}%)", end="", flush=True)

    urllib.request.urlretrieve(url, tmp, reporthook)
    print()
    os.rename(tmp, dest)


def write_fbin(path: str, data: np.ndarray) -> None:
    """Write float32 vectors in binary format."""
    n, dim = data.shape
    data = data.astype(np.float32)
    with open(path, "wb") as f:
        f.write(struct.pack("<ii", n, dim))
        f.write(data.tobytes())
    print(f"  Wrote {path}: {n} vectors, {dim}d ({os.path.getsize(path) / 1e6:.1f} MB)")


def write_ibin(path: str, data: np.ndarray) -> None:
    """Write int32 ground truth in binary format."""
    n, k = data.shape
    data = data.astype(np.int32)
    with open(path, "wb") as f:
        f.write(struct.pack("<ii", n, k))
        f.write(data.tobytes())
    print(f"  Wrote {path}: {n} queries, k={k}")


def convert_dataset(name: str, info: dict, output_dir: str) -> None:
    """Download HDF5 and convert to binary format."""
    dataset_dir = os.path.join(output_dir, name)
    os.makedirs(dataset_dir, exist_ok=True)

    hdf5_path = os.path.join(dataset_dir, f"{name}.hdf5")
    download_file(info["url"], hdf5_path)

    print(f"  Converting {name}...")
    with h5py.File(hdf5_path, "r") as f:
        train = np.array(f["train"])
        test = np.array(f["test"])
        neighbors = np.array(f["neighbors"])

        print(f"  Base vectors: {train.shape}")
        print(f"  Query vectors: {test.shape}")
        print(f"  Ground truth: {neighbors.shape}")

        write_fbin(os.path.join(dataset_dir, "base.fbin"), train)
        write_fbin(os.path.join(dataset_dir, "queries.fbin"), test)

        # Ground truth: take top-100 (or whatever's available)
        k = min(100, neighbors.shape[1])
        write_ibin(
            os.path.join(dataset_dir, "groundtruth.ibin"), neighbors[:, :k]
        )

    # Write metadata
    meta_path = os.path.join(dataset_dir, "metadata.txt")
    with open(meta_path, "w") as f:
        f.write(f"name: {name}\n")
        f.write(f"metric: {info['metric']}\n")
        f.write(f"dim: {info['dim']}\n")
        f.write(f"base_vectors: {train.shape[0]}\n")
        f.write(f"query_vectors: {test.shape[0]}\n")
        f.write(f"ground_truth_k: {k}\n")

    print(f"  Done: {dataset_dir}/")


def main():
    parser = argparse.ArgumentParser(
        description="Download standard ANN benchmark datasets"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        help=f"Dataset name. Available: {', '.join(DATASETS.keys())}",
    )
    parser.add_argument(
        "--output", type=str, default="datasets", help="Output directory"
    )
    parser.add_argument(
        "--list", action="store_true", help="List available datasets"
    )
    parser.add_argument(
        "--all", action="store_true", help="Download all datasets"
    )
    args = parser.parse_args()

    if args.list:
        print("Available datasets:")
        for name, info in DATASETS.items():
            print(f"  {name:25s} {info['description']}")
        return

    if args.all:
        for name, info in DATASETS.items():
            print(f"\n=== {name} ===")
            try:
                convert_dataset(name, info, args.output)
            except Exception as e:
                print(f"  ERROR: {e}", file=sys.stderr)
        return

    if not args.dataset:
        # Default: download sift-128 and glove-100
        for name in ["sift-128", "glove-100"]:
            print(f"\n=== {name} ===")
            convert_dataset(name, DATASETS[name], args.output)
        return

    if args.dataset not in DATASETS:
        print(
            f"Unknown dataset: {args.dataset}. Available: {', '.join(DATASETS.keys())}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"\n=== {args.dataset} ===")
    convert_dataset(args.dataset, DATASETS[args.dataset], args.output)


if __name__ == "__main__":
    main()
