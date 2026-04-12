"""Option A: difflib + iterrows (stdlib only)."""

import json
import time
import tracemalloc
from difflib import SequenceMatcher

ROW_LIMIT = 1000  # Cap to avoid 8+ minute runs


def clean(s):
    return str(s).strip().lower().rstrip(".,")


def run(source, target):
    results = {}
    target_names = [r["vendor_name"] for r in target]

    # TC-01: Fuzzy name matching
    tracemalloc.start()
    t0 = time.time()
    matches_01 = []
    for row in source[:ROW_LIMIT]:
        src = clean(row["l3_fmly_nm"])
        best_score, best_match = 0, None
        for tgt in target_names:
            score = SequenceMatcher(None, src, clean(tgt)).ratio()
            if score > best_score:
                best_score, best_match = score, tgt
        if best_score >= 0.85:
            matches_01.append((row["vendor_id"], best_match, best_score))
    t_01 = time.time() - t0
    mem_01 = tracemalloc.get_traced_memory()[1] / 1024 / 1024
    tracemalloc.stop()

    results["TC-01"] = {
        "time_s": round(t_01, 3),
        "projected_15k_s": round(t_01 * (15000 / ROW_LIMIT), 1),
        "memory_mb": round(mem_01, 1),
        "matches": len(matches_01),
        "rows_tested": ROW_LIMIT,
    }

    # TC-02: Clean + exact match
    tracemalloc.start()
    t0 = time.time()
    target_clean = {clean(r["vendor_name"]): r for r in target}
    matches_02 = []
    for row in source[:ROW_LIMIT]:
        src = clean(row["l3_fmly_nm"])
        if src in target_clean:
            matches_02.append((row["vendor_id"], target_clean[src]["vendor_name"]))
    t_02 = time.time() - t0
    mem_02 = tracemalloc.get_traced_memory()[1] / 1024 / 1024
    tracemalloc.stop()

    results["TC-02"] = {
        "time_s": round(t_02, 3),
        "projected_15k_s": round(t_02 * (15000 / ROW_LIMIT), 2),
        "memory_mb": round(mem_02, 1),
        "matches": len(matches_02),
        "rows_tested": ROW_LIMIT,
    }

    # TC-03: Address token overlap
    tracemalloc.start()
    t0 = time.time()
    matches_03 = {"0-59": 0, "60-79": 0, "80-99": 0, "100": 0}
    for row in source[:ROW_LIMIT]:
        src_addr = clean(row["hq_addr1"] + " " + row["hq_addr2"])
        src_tokens = set(src_addr.split())
        best_score = 0
        for tgt in target:
            tgt_addr = clean(tgt["address1"] + " " + tgt["address2"])
            tgt_tokens = set(tgt_addr.split())
            if not src_tokens or not tgt_tokens:
                continue
            overlap = len(src_tokens & tgt_tokens) / max(len(src_tokens), len(tgt_tokens))
            if overlap > best_score:
                best_score = overlap
        pct = int(best_score * 100)
        if pct == 100:
            matches_03["100"] += 1
        elif pct >= 80:
            matches_03["80-99"] += 1
        elif pct >= 60:
            matches_03["60-79"] += 1
        else:
            matches_03["0-59"] += 1
    t_03 = time.time() - t0
    mem_03 = tracemalloc.get_traced_memory()[1] / 1024 / 1024
    tracemalloc.stop()

    results["TC-03"] = {
        "time_s": round(t_03, 3),
        "projected_15k_s": round(t_03 * (15000 / ROW_LIMIT), 1),
        "memory_mb": round(mem_03, 1),
        "score_distribution": matches_03,
        "rows_tested": ROW_LIMIT,
    }

    # TC-04: Combined pipeline
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    tracemalloc.start()
    t0 = time.time()
    matches_04 = []
    date_filtered = 0
    # Only filter DESTINATION records by date — Pop1's cntrct_cmpl_dt is invalid
    valid_targets = [tgt for tgt in target if tgt["updated"] >= cutoff]
    date_filtered = len(target) - len(valid_targets)
    for row in source[:ROW_LIMIT]:
        src = clean(row["l3_fmly_nm"])
        best_score, best_match = 0, None
        for tgt in valid_targets:
            score = SequenceMatcher(None, src, clean(tgt["vendor_name"])).ratio()
            if score > best_score:
                best_score, best_match = score, tgt["vendor_name"]
        if best_score >= 0.85:
            matches_04.append((row["vendor_id"], best_match, best_score))
    t_04 = time.time() - t0
    mem_04 = tracemalloc.get_traced_memory()[1] / 1024 / 1024
    tracemalloc.stop()

    results["TC-04"] = {
        "time_s": round(t_04, 3),
        "projected_15k_s": round(t_04 * (15000 / ROW_LIMIT), 1),
        "memory_mb": round(mem_04, 1),
        "matches": len(matches_04),
        "date_filtered": date_filtered,
        "rows_tested": ROW_LIMIT,
    }

    return results


if __name__ == "__main__":
    with open("benchmarks/results/source.json") as f:
        source = json.load(f)
    with open("benchmarks/results/target.json") as f:
        target = json.load(f)

    results = run(source, target)
    with open("benchmarks/results/option_a.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Option A complete → benchmarks/results/option_a.json")
