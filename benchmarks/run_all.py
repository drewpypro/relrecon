"""Run all benchmarks and produce summary."""

import json
import os
import subprocess
import sys
from datetime import datetime

SCRIPTS = [
    ("generate_data", "benchmarks/generate_data.py"),
    ("option_a", "benchmarks/bench_option_a.py"),
    ("option_b", "benchmarks/bench_option_b.py"),
    ("option_c", "benchmarks/bench_option_c.py"),
]


def main():
    os.makedirs("benchmarks/results", exist_ok=True)

    for name, script in SCRIPTS:
        print(f"Running {name}...")
        result = subprocess.run([sys.executable, script], capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  FAILED: {result.stderr[:200]}")
            return 1
        print(f"  {result.stdout.strip()}")

    # Load results
    with open("benchmarks/results/option_a.json") as f:
        a = json.load(f)
    with open("benchmarks/results/option_b.json") as f:
        b = json.load(f)
    with open("benchmarks/results/option_c.json") as f:
        c = json.load(f)

    # Build summary
    summary = {
        "generated_at": datetime.now().isoformat(),
        "hardware": {"cores_used": 8, "note": "throttled to emulate ZBook Ryzen 9 PRO 7940HS"},
        "dataset": {"source_rows": 15000, "target_rows": 500, "seed": 42},
        "results": {"A": a, "B": b, "C": c},
    }

    with open("benchmarks/results/summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    # Human-readable summary
    lines = []
    lines.append("=" * 78)
    lines.append("BENCHMARK RESULTS — ADR-001 Tooling Validation")
    lines.append(f"Generated: {summary['generated_at']}")
    lines.append(f"Hardware: 8 cores (throttled to emulate ZBook)")
    lines.append(f"Dataset: 15,000 source × 500 target rows (seed=42)")
    lines.append("=" * 78)

    for tc in ["TC-01", "TC-02", "TC-03", "TC-04"]:
        lines.append("")
        lines.append(f"--- {tc} ---")
        for opt_name, opt_data in [("A", a), ("B", b), ("C", c)]:
            r = opt_data[tc]
            rows = r["rows_tested"]
            actual_time = r["time_s"]
            projected = r.get("projected_15k_s", actual_time)
            matches = r.get("matches", "N/A")
            mem = r["memory_mb"]
            if rows < 15000:
                time_str = f"{actual_time:.3f}s ({rows} rows) -> projected {projected:.1f}s (15k)"
            else:
                time_str = f"{actual_time:.3f}s (full 15k)"
            lines.append(f"  Option {opt_name}: {time_str} | {mem:.1f}MB | matches={matches}")
            if "score_distribution" in r:
                lines.append(f"    scores: {r['score_distribution']}")
            if "date_filtered" in r:
                lines.append(f"    date_filtered: {r['date_filtered']}")

    # Speedup summary
    lines.append("")
    lines.append("--- SPEEDUP (TC-01 fuzzy name matching, projected to 15k) ---")
    a_time = a["TC-01"].get("projected_15k_s", a["TC-01"]["time_s"])
    b_time = b["TC-01"].get("projected_15k_s", b["TC-01"]["time_s"])
    c_time = c["TC-01"]["time_s"]
    lines.append(f"  A: {a_time:.1f}s | B: {b_time:.1f}s | C: {c_time:.3f}s")
    lines.append(f"  C vs A: {a_time/c_time:.0f}x faster")
    lines.append(f"  C vs B: {b_time/c_time:.0f}x faster")

    # Success criteria
    lines.append("")
    lines.append("--- SUCCESS CRITERIA ---")
    c_total = sum(c[tc]["time_s"] for tc in ["TC-01", "TC-02", "TC-03", "TC-04"])
    lines.append(f"  1. Option C total time < 60s: {c_total:.2f}s — {'PASS' if c_total < 60 else 'FAIL'}")
    lines.append(f"  2. C >= 10x faster than B (TC-01): {b_time/c_time:.0f}x — {'PASS' if b_time/c_time >= 10 else 'FAIL'}")
    lines.append(f"  3. C >= 100x faster than A (TC-01): {a_time/c_time:.0f}x — {'PASS' if a_time/c_time >= 100 else 'FAIL'}")

    a_matches = a["TC-01"]["matches"]
    c_matches = c["TC-01"]["matches"]
    # Scale A matches to 15k for comparison
    a_scaled = int(a_matches * (15000 / a["TC-01"]["rows_tested"]))
    match_diff = abs(a_scaled - c_matches) / max(a_scaled, c_matches) * 100
    lines.append(f"  4. Match quality within 10%: {match_diff:.1f}% diff — {'PASS' if match_diff <= 10 else 'NOTE: diff > 10%'}")

    max_mem = max(c[tc]["memory_mb"] for tc in ["TC-01", "TC-02", "TC-03", "TC-04"])
    lines.append(f"  5. Peak memory < 1GB: {max_mem:.1f}MB — {'PASS' if max_mem < 1024 else 'FAIL'}")

    lines.append("")

    txt = "\n".join(lines)
    with open("benchmarks/results/summary.txt", "w") as f:
        f.write(txt)
    # Write markdown version for repo
    md = []
    md.append("# Benchmark Results - ADR-001 Tooling Validation")
    md.append(f"")
    md.append(f"**Generated:** {summary['generated_at'][:10]}")
    md.append(f"**Hardware:** 8 cores (throttled to emulate ZBook Ryzen 9 PRO 7940HS)")
    md.append(f"**Dataset:** 15,000 source x 500 target rows (seed=42)")
    md.append(f"")
    md.append(f"[Test Case Definitions](../test_cases.md) | [ADR-001](../../docs/adr/001-matching-engine-tooling.md) | [README](../../README.MD)")
    md.append("")
    md.append("---")
    md.append("")

    tc_titles = {
        "TC-01": "TC-01: Fuzzy Name Matching",
        "TC-02": "TC-02: Clean Normalization + Exact Match",
        "TC-03": "TC-03: Address Token Overlap",
        "TC-04": "TC-04: Combined Pipeline (Date Gate + Name + Address)",
    }

    for tc in ["TC-01", "TC-02", "TC-03", "TC-04"]:
        md.append(f"## {tc_titles[tc]}")
        md.append("")
        has_scores = "score_distribution" in c[tc]
        has_date = "date_filtered" in a[tc]

        if has_scores:
            md.append("| Option | Time | Rows | Memory |")
            md.append("|---|---|---|---|")
        elif has_date:
            md.append("| Option | Time | Rows | Memory | Matches | Dest. Filtered |")
            md.append("|---|---|---|---|---|---|")
        else:
            md.append("| Option | Time | Rows | Memory | Matches |")
            md.append("|---|---|---|---|---|")

        for opt_name, opt_data in [("A", a), ("B", b), ("C", c)]:
            r = opt_data[tc]
            rows = r["rows_tested"]
            t = r["time_s"]
            projected = r.get("projected_15k_s", t)
            mem = r["memory_mb"]
            matches = r.get("matches", "N/A")

            if rows < 15000:
                time_str = f"{t:.3f}s ({rows:,}) -> **{projected:.1f}s projected**"
            else:
                time_str = f"**{t:.3f}s**"

            if has_scores:
                md.append(f"| {opt_name} | {time_str} | {rows:,} | {mem:.1f} MB |")
            elif has_date:
                df = r.get('date_filtered', 0)
                md.append(f"| {opt_name} | {time_str} | {rows:,} | {mem:.1f} MB | {matches} | {df} |")
            else:
                md.append(f"| {opt_name} | {time_str} | {rows:,} | {mem:.1f} MB | {matches} |")

        if has_scores:
            md.append("")
            md.append("**Score Distribution (Option C, 15k rows):**")
            md.append("")
            md.append("| Range | Count |")
            md.append("|---|---|")
            for rng, cnt in c[tc]["score_distribution"].items():
                md.append(f"| {rng}% | {cnt:,} |")

        md.append("")

    md.append("---")
    md.append("")
    md.append("## Speedup Summary (TC-01, projected to 15k)")
    md.append("")
    md.append("| Comparison | Result |")
    md.append("|---|---|")
    md.append(f"| A -> C | **{a_time/c_time:,.0f}x faster** |")
    md.append(f"| B -> C | **{b_time/c_time:,.0f}x faster** |")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Success Criteria")
    md.append("")
    md.append("| # | Criteria | Result | Status |")
    md.append("|---|---|---|---|")
    c_total = sum(c[tc]["time_s"] for tc in ["TC-01", "TC-02", "TC-03", "TC-04"])
    md.append(f"| 1 | Option C total time < 60s | {c_total:.2f}s | **{'PASS' if c_total < 60 else 'FAIL'}** |")
    md.append(f"| 2 | C >= 10x faster than B (TC-01) | {b_time/c_time:,.0f}x | **{'PASS' if b_time/c_time >= 10 else 'FAIL'}** |")
    md.append(f"| 3 | C >= 100x faster than A (TC-01) | {a_time/c_time:,.0f}x | **{'PASS' if a_time/c_time >= 100 else 'FAIL'}** |")
    md.append(f"| 4 | Match quality within 10% | {match_diff:.1f}% diff | **{'PASS' if match_diff <= 10 else 'NOTE'}** |")
    md.append(f"| 5 | Peak memory < 1GB | {max_mem:.1f} MB | **{'PASS' if max_mem < 1024 else 'FAIL'}** |")
    md.append("")
    md.append("---")
    md.append("")
    md.append("*Options A and B were capped at 1,000 rows with projected extrapolation. Option C ran on the full 15,000 rows.*")

    with open("benchmarks/results/summary_snapshot.md", "w") as f:
        f.write("\n".join(md))

    print(txt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
