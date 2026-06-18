#!/usr/bin/env python3
"""Collect K6_SUMMARY lines from k6-operator runner pods and merge the per-step curves."""

import json
import re
import subprocess
import argparse


def pod_logs(namespace, pod):
    result = subprocess.run(
        ["kubectl", "logs", pod, "--namespace", namespace],
        capture_output=True, text=True,
    )
    return result.stdout


def collect(namespace, testrun):
    result = subprocess.run(
        ["kubectl", "get", "pods", "--namespace", namespace,
         "-l", f"k6_cr={testrun},runner=true", "-o", "jsonpath={.items[*].metadata.name}"],
        capture_output=True, text=True, check=True,
    )
    pods = result.stdout.split()
    if not pods:
        raise SystemExit(f"no pods found for testrun={testrun}")

    summaries = []
    for pod in pods:
        logs = pod_logs(namespace, pod)
        for line in logs.splitlines():
            m = re.search(r'K6_SUMMARY:(\{.*\})"?', line)
            if m:
                raw = m.group(1).replace('\\"', '"')
                summaries.append(json.loads(raw))
                break

    return summaries


def merge(summaries):
    if not summaries:
        raise SystemExit("no K6_SUMMARY lines found in pod logs")

    steps = summaries[0]["steps"]
    curve = []
    for i in range(steps):
        # offered_rps is the total system target for this step
        offered   = summaries[0]["curve"][i]["offered_rps"]
        committed = sum(s["curve"][i]["committed_rps"] for s in summaries)
        rejected  = sum(s["curve"][i]["rejected_rps"]  for s in summaries)
        rej429    = sum(s["curve"][i].get("rej429_rps", 0)    for s in summaries)
        rej503    = sum(s["curve"][i].get("rej503_rps", 0)    for s in summaries)
        rej_other = sum(s["curve"][i].get("rej_other_rps", 0) for s in summaries)
        lats      = [s["curve"][i]["mean_lat_ms"] for s in summaries if s["curve"][i]["committed_rps"] > 0]
        curve.append({
            "offered_rps":   offered,
            "committed_rps": committed,
            "rejected_rps":  rejected,
            "rej429_rps":    rej429,
            "rej503_rps":    rej503,
            "rej_other_rps": rej_other,
            "mean_lat_ms":   sum(lats) / len(lats) if lats else 0,
        })

    peak = max(curve, key=lambda c: c["committed_rps"])

    return {
        "workers":            len(summaries),
        "max_rps":            summaries[0]["max_rps"],
        "committed_total":    sum(s["committed_total"]    for s in summaries),
        "rejected_total":     sum(s.get("rejected_total", 0) for s in summaries),
        "rej429_total":       sum(s.get("rej429_total", 0)    for s in summaries),
        "rej503_total":       sum(s.get("rej503_total", 0)    for s in summaries),
        "rej_other_total":    sum(s.get("rej_other_total", 0) for s in summaries),
        "dropped_iterations": sum(s["dropped_iterations"] for s in summaries),
        "peak_committed_rps": peak["committed_rps"],
        "peak_offered_rps":   peak["offered_rps"],
        "curve":              curve,
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--namespace", default="bench")
    p.add_argument("--testrun",   default="k6-sweep")
    p.add_argument("--out",       required=True)
    args = p.parse_args()

    summaries = collect(args.namespace, args.testrun)
    merged    = merge(summaries)

    with open(args.out, "w") as f:
        json.dump(merged, f, indent=2)

    print(f"merged {merged['workers']} workers → "
          f"peak {merged['peak_committed_rps']:.0f} rps committed "
          f"at {merged['peak_offered_rps']} rps offered  "
          f"(dropped={merged['dropped_iterations']})")
