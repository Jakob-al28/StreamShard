#!/usr/bin/env python3
"""
Parse k6 sweep result files and produce:
  1. Throughput-vs-offered-load curves (rise/peak/collapse), one line per config
  2. Peak-throughput vs node-count plot with ideal-linear reference + USL fit

Usage:
    python usl.py results/1node.json results/3node.json results/5node.json --nodes 1 3 5

Each file is the merged summary from aggregate.py:
    { "peak_committed_rps": R, "curve": [{offered_rps, committed_rps, ...}], ... }
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit


def load(path: Path):
    with open(path) as f:
        return json.load(f)


def peak_throughput(d, nodes):
    peak = d.get("peak_committed_rps", 0)
    peak_at = d.get("peak_offered_rps", 0)
    if peak_at >= d.get("max_rps", 0) * 0.95:
        print(f"  WARNING: {nodes}-node peak is at the top of the ramp ({peak_at} rps offered) — "
              f"the true ceiling may be higher. Raise MAX_RPS and re-run.", file=sys.stderr)
    return peak


def usl(n, alpha, beta):
    return n / (1 + alpha * (n - 1) + beta * n * (n - 1))


def fit_usl(nodes, throughputs):
    x1 = throughputs[0] if throughputs[0] > 0 else 1.0
    normalized = [t / x1 for t in throughputs]
    try:
        popt, _ = curve_fit(
            usl, nodes, normalized,
            p0=[0.1, 0.01], bounds=([0, 0], [1, 1]), maxfev=10000,
        )
        alpha, beta = popt[0], popt[1]
    except RuntimeError:
        return None, None
    if alpha < 1e-6 and beta < 1e-6:
        return None, None
    return alpha, beta


def plot_curves(data, node_counts, title, out, xmax=None, ymax=None):
    fig, ax = plt.subplots(figsize=(8, 5))
    for d, n in zip(data, node_counts):
        xs = [c["offered_rps"]   for c in d["curve"]]
        ys = [c["committed_rps"] for c in d["curve"]]
        ax.plot(xs, ys, "o-", label=f"{n} node(s)", markersize=4)
    max_offered = max(c["offered_rps"] for d in data for c in d["curve"])
    line_max = xmax if xmax else max_offered
    ax.plot([0, line_max], [0, line_max],
            "--", color="gray", alpha=0.4, label="offered = committed")
    if xmax:
        ax.set_xlim(0, xmax)
    if ymax:
        ax.set_ylim(0, ymax)
    ax.set_xlabel("offered load (rps)")
    ax.set_ylabel("committed writes / s")
    ax.set_title(f"{title}")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"curve plot saved to {out}")


def plot_scaling(node_counts, peaks, alpha, beta, title, out, ymax=None):
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(node_counts, peaks, "o-", label="measured peak", zorder=3)

    ideal = [peaks[0] * n for n in node_counts]
    ax.plot(node_counts, ideal, "--", color="gray", alpha=0.5, label="ideal linear")

    if alpha is not None:
        ns = np.linspace(min(node_counts), max(node_counts) * 1.2, 200)
        fitted = [usl(n, alpha, beta) * peaks[0] for n in ns]
        ax.plot(ns, fitted, ":", color="red", alpha=0.7,
                label=f"USL fit (α={alpha:.3f}, β={beta:.5f})")

    for n, t, it in zip(node_counts, peaks, ideal):
        eff = t / it * 100 if it > 0 else 0
        ax.annotate(f"{eff:.0f}%", xy=(n, t), xytext=(4, 6),
                    textcoords="offset points", fontsize=8)

    if ymax:
        ax.set_ylim(0, ymax)
    ax.set_xlabel("nodes")
    ax.set_ylabel("peak committed writes / s")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"scaling plot saved to {out}")


def main():
    parser = argparse.ArgumentParser(description="Load-sweep + USL analysis for StreamShard")
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--nodes", nargs="+", type=int)
    parser.add_argument("--out", type=Path, default=Path("scaling_curve.png"))
    parser.add_argument("--title", default="StreamShard throughput scaling")
    parser.add_argument("--xmax", type=float, default=None,
                        help="clip the sweep plot x-axis (offered rps) for readability; does not affect peaks")
    parser.add_argument("--ymax", type=float, default=None,
                        help="fix the y-axis (committed rps) on both plots, so curve heights are comparable across configs")
    args = parser.parse_args()

    node_counts = args.nodes if args.nodes else [1, 3, 5]
    if len(node_counts) != len(args.files):
        print(f"error: {len(args.files)} files but {len(node_counts)} node counts", file=sys.stderr)
        sys.exit(1)

    data = [load(p) for p in args.files]
    peaks = []
    for d, n in zip(data, node_counts):
        p = peak_throughput(d, n)
        peaks.append(p)
        print(f"  {n} node(s): peak {p:.1f} commits/s")

    alpha, beta = fit_usl(node_counts, peaks)
    if alpha is not None:
        print(f"\nUSL fit:  alpha={alpha:.4f} (contention)  beta={beta:.6f} (coherency)")
        ceiling_n = int(np.sqrt((1 - alpha) / beta)) if beta > 0 else 999
        print(f"  predicted ceiling at n={ceiling_n}")
    else:
        print("\nUSL fit skipped")

    curves_out = args.out.with_name(args.out.stem + "_sweep" + args.out.suffix)
    plot_curves(data, node_counts, args.title, curves_out, args.xmax, args.ymax)
    plot_scaling(node_counts, peaks, alpha, beta, args.title, args.out, args.ymax)


if __name__ == "__main__":
    main()
