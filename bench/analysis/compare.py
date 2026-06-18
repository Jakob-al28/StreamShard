#!/usr/bin/env python3
"""Overlay the offered-vs-committed sweep curves of two (or more) result files on one
axis, mark each peak, and print the peak deltas. For A/B comparisons at a fixed node
count (e.g. wal-batch on vs off, gateway-fanout vs primary-replication).

Usage:
  python compare.py A.json B.json --labels "batch1" "batch64" \
    --title "1-node e2-standard-4, WAL batching" --xmax 16000 --out cmp.png
"""

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def load(p):
    with open(p) as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+", type=Path)
    ap.add_argument("--labels", nargs="+", default=None,
                    help="legend label per file (defaults to file stem)")
    ap.add_argument("--title", default="Throughput comparison")
    ap.add_argument("--xmax", type=float, default=None,
                    help="clip x-axis (offered rps); does not affect reported peaks")
    ap.add_argument("--out", type=Path, default=Path("compare.png"))
    args = ap.parse_args()

    labels = args.labels or [f.stem for f in args.files]
    if len(labels) != len(args.files):
        raise SystemExit(f"{len(args.files)} files but {len(labels)} labels")

    data = [load(f) for f in args.files]

    fig, ax = plt.subplots(figsize=(8, 5))
    peaks = []
    for d, lbl in zip(data, labels):
        xs = [c["offered_rps"] for c in d["curve"]]
        ys = [c["committed_rps"] for c in d["curve"]]
        ax.plot(xs, ys, "o-", markersize=4, label=lbl)
        peak = max(d["curve"], key=lambda c: c["committed_rps"])
        peaks.append((lbl, peak["committed_rps"], peak["offered_rps"]))

    max_off = max(c["offered_rps"] for d in data for c in d["curve"])
    line_max = args.xmax if args.xmax else max_off
    ax.plot([0, line_max], [0, line_max], "--", color="gray", alpha=0.4,
            label="offered = committed")
    if args.xmax:
        ax.set_xlim(0, args.xmax)

    ax.set_xlabel("offered load (rps)")
    ax.set_ylabel("committed writes / s")
    ax.set_title(args.title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(args.out, dpi=150)

    print(f"saved {args.out}")
    for lbl, pk, at in peaks:
        print(f"  {lbl:<20} peak {pk:>9.1f} committed/s  @ {at} offered")
    if len(peaks) == 2:
        base, other = peaks[0][1], peaks[1][1]
        if base > 0:
            print(f"  delta: {peaks[1][0]} vs {peaks[0][0]} = "
                  f"{(other - base):+.0f}/s ({(other / base - 1) * 100:+.1f}%)")


if __name__ == "__main__":
    main()
