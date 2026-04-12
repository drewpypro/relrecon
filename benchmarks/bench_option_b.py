"""Option B: pandas + fuzzywuzzy."""

import json
import time
import tracemalloc
import pandas as pd
from fuzzywuzzy import fuzz as fwfuzz

ROW_LIMIT = 1000  # Cap to avoid long runs


def clean(s):
    return str(s).strip().lower().rstrip(".,")


def run(source, target):
    results = {}
    source_df = pd.DataFrame(source)
    target_df = pd.DataFrame(target)
    target_names = target_df["vendor_name"].tolist()

    # TC-01: Fuzzy name matching
    tracemalloc.start()
    t0 = time.time()
    matches_01 = []
    for idx, row in source_df.head(ROW_LIMIT).iterrows():
        src = clean(row["l3_fmly_nm"])
        best_score, best_match = 0, None
        for tgt in target_names:
            score = fwfuzz.ratio(src, clean(tgt)) / 100.0
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

    # TC-02: Clean + exact match via pandas merge
    tracemalloc.start()
    t0 = time.time()
    src_df = source_df.head(ROW_LIMIT).copy()
    src_df["name_clean"] = src_df["l3_fmly_nm"].apply(clean)
    tgt_df = target_df.copy()
    tgt_df["name_clean"] = tgt_df["vendor_name"].apply(clean)
    merged = src_df.merge(tgt_df, on="name_clean", how="inner")
    # Deduplicate: one match per source row (avoid cartesian product inflation)
    merged = merged.drop_duplicates(subset=["vendor_id_x"])
    t_02 = time.time() - t0
    mem_02 = tracemalloc.get_traced_memory()[1] / 1024 / 1024
    tracemalloc.stop()

    results["TC-02"] = {
        "time_s": round(t_02, 3),
        "projected_15k_s": round(t_02 * (15000 / ROW_LIMIT), 2),
        "memory_mb": round(mem_02, 1),
        "matches": len(merged),
        "rows_tested": ROW_LIMIT,
    }

    # TC-03: Address token overlap
    tracemalloc.start()
    t0 = time.time()
    matches_03 = {"0-59": 0, "60-79": 0, "80-99": 0, "100": 0}
    for idx, row in source_df.head(ROW_LIMIT).iterrows():
        src_addr = clean(str(row["hq_addr1"]) + " " + str(row["hq_addr2"]))
        best_score = 0
        for _, tgt in target_df.iterrows():
            tgt_addr = clean(str(tgt["address1"]) + " " + str(tgt["address2"]))
            score = fwfuzz.token_sort_ratio(src_addr, tgt_addr) / 100.0
            if score > best_score:
                best_score = score
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
    # Only filter DESTINATION records by date — Pop1's cntrct_cmpl_dt is invalid
    valid_targets = target_df[target_df["updated"] >= cutoff]
    date_filtered = len(target_df) - len(valid_targets)
    valid_target_names = valid_targets["vendor_name"].tolist()
    for idx, row in source_df.head(ROW_LIMIT).iterrows():
        src = clean(row["l3_fmly_nm"])
        best_score, best_match = 0, None
        for tgt in valid_target_names:
            score = fwfuzz.ratio(src, clean(tgt)) / 100.0
            if score > best_score:
                best_score, best_match = score, tgt
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
    with open("benchmarks/results/option_b.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Option B complete → benchmarks/results/option_b.json")
