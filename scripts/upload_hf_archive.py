#!/usr/bin/env python3
"""Upload the emojistats parquet archive to a Hugging Face dataset repo."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from typing import Any


DEFAULT_PATTERNS = [
    "shard0/*.parquet",
    "shard0/**/*.parquet",
    "shard1/*.parquet",
    "shard1/**/*.parquet",
    "shard2/*.parquet",
    "shard2/**/*.parquet",
    "shard3/*.parquet",
    "shard3/**/*.parquet",
    "shard4/*.parquet",
    "shard4/**/*.parquet",
    "shard5/*.parquet",
    "shard5/**/*.parquet",
    "v1-recrawl/*.parquet",
    "v1-recrawl/**/*.parquet",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload parquet files from a mounted emojistats archive root using "
            "huggingface_hub.upload_large_folder."
        )
    )
    parser.add_argument(
        "--repo-id",
        required=True,
        help="Hugging Face dataset repo id, e.g. alice/emojistats-bsky.",
    )
    parser.add_argument(
        "--archive-root",
        required=True,
        type=Path,
        help="Mounted storagebox archive root containing shard0..5, v1-recrawl, live.",
    )
    parser.add_argument(
        "--dataset-card",
        type=Path,
        default=Path("docs/huggingface-dataset-card.md"),
        help="README.md content to upload to the dataset repo.",
    )
    parser.add_argument(
        "--revision",
        default=None,
        help="Target branch or revision. Defaults to main.",
    )
    parser.add_argument(
        "--private",
        action="store_true",
        help="Create the dataset repo as private if it does not exist.",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=4,
        help="Upload worker count. Keep low when reading through rclone/FUSE.",
    )
    parser.add_argument(
        "--allow-pattern",
        action="append",
        dest="allow_patterns",
        help=(
            "Override upload allow patterns. Repeatable. Defaults to shard0..5, "
            "v1-recrawl, and live parquet files."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned files and total bytes without creating/uploading.",
    )
    return parser.parse_args()


def require_archive_root(root: Path) -> Path:
    root = root.expanduser().resolve()
    if not root.is_dir():
        raise SystemExit(f"archive root is not a directory: {root}")
    missing = [name for name in ["shard0", "shard5", "v1-recrawl"] if not (root / name).is_dir()]
    if missing:
        raise SystemExit(f"archive root is missing expected dirs: {', '.join(missing)}")
    return root


def iter_matching_files(root: Path, patterns: list[str]) -> list[Path]:
    files: dict[Path, None] = {}
    for pattern in patterns:
        for path in root.glob(pattern):
            if path.is_file():
                files[path] = None
    return sorted(files)


def print_dry_run(root: Path, patterns: list[str]) -> None:
    files = iter_matching_files(root, patterns)
    total = sum(path.stat().st_size for path in files)
    print(f"archive_root={root}")
    print(f"patterns={patterns}")
    print(f"files={len(files)}")
    print(f"bytes={total}")
    print(f"gib={total / (1024**3):.3f}")
    for path in files[:10]:
        print(f"sample={path.relative_to(root)}")
    if len(files) > 10:
        print(f"... {len(files) - 10} more")


def upload_dataset_card(
    api: Any,
    repo_id: str,
    dataset_card: Path,
    revision: str | None,
) -> None:
    if not dataset_card.exists():
        print(f"dataset card not found, skipping: {dataset_card}", file=sys.stderr)
        return
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as tmp:
        tmp.write(dataset_card.read_text(encoding="utf-8"))
        tmp_path = Path(tmp.name)
    try:
        api.upload_file(
            repo_id=repo_id,
            repo_type="dataset",
            revision=revision,
            path_or_fileobj=tmp_path,
            path_in_repo="README.md",
            commit_message="Add dataset card",
        )
    finally:
        tmp_path.unlink(missing_ok=True)


def main() -> None:
    args = parse_args()
    root = require_archive_root(args.archive_root)
    allow_patterns = args.allow_patterns or DEFAULT_PATTERNS

    if args.dry_run:
        print_dry_run(root, allow_patterns)
        return

    try:
        from huggingface_hub import HfApi
    except ModuleNotFoundError as err:
        raise SystemExit(
            "missing dependency: pip install -U huggingface_hub",
        ) from err

    if not os.environ.get("HF_TOKEN"):
        print(
            "HF_TOKEN is not set. Export a write token or run `hf auth login` first.",
            file=sys.stderr,
        )

    os.environ.setdefault("HF_XET_HIGH_PERFORMANCE", "1")
    api = HfApi()
    api.create_repo(
        repo_id=args.repo_id,
        repo_type="dataset",
        private=args.private,
        exist_ok=True,
    )
    upload_dataset_card(api, args.repo_id, args.dataset_card, args.revision)
    api.upload_large_folder(
        repo_id=args.repo_id,
        repo_type="dataset",
        revision=args.revision,
        private=args.private,
        folder_path=root,
        allow_patterns=allow_patterns,
        ignore_patterns=[".cache/**"],
        num_workers=args.num_workers,
        print_report=True,
        print_report_every=60,
    )


if __name__ == "__main__":
    main()
