"""Option C: polars + rapidfuzz."""

import json
import os
import time
import tracemalloc

os.environ["POLARS_MAX_THREADS"] = "8"

import polars as pl
from rapidfuzz import fuzz as rfuzz, process as rprocess


def clean(s):
    return str(s).strip().lower().rstrip(".,")


def run(source, target):
    results = {}
    source_pl = pl.DataFrame(source)
    target_pl = pl.DataFrame(target)
    target_names = target_pl["vendor_name"].to_list()
    target_names_clean = [clean(n) for n in target_names]

    # TC-01: Fuzzy name matching (FULL 15k)
    tracemalloc.start()
    t0 = time.time()
    matches_01 = []
    for name, vid in zip(source_pl["l3_fmly_nm"].to_list(), source_pl["vendor_id"].to_list()):
        result = rprocess.extractOne(
            clean(name), target_names_clean,
            scorer=rfuzz.ratio, score_cutoff=85
        )
        if result:
            matches_01.append((vid, target_names[result[2]], result[1] / 100.0))
    t_01 = time.time() - t0
    mem_01 = tracemalloc.get_traced_memory()[1] / 1024 / 1024
    tracemalloc.stop()

    results["TC-01"] = {
        "time_s": round(t_01, 3),
        "memory_mb": round(mem_01, 1),
        "matches": len(matches_01),
        "rows_tested": len(source),
    }

    # TC-02: Clean + exact match via polars join (FULL 15k)
    tracemalloc.start()
    t0 = time.time()
    src_with_clean = source_pl.with_columns(
        pl.col("l3_fmly_nm").str.strip_chars().str.to_lowercase()
        .str.strip_chars_end(".,").alias("name_clean")
    )
    tgt_with_clean = target_pl.with_columns(
        pl.col("vendor_name").str.strip_chars().str.to_lowercase()
        .str.strip_chars_end(".,").alias("name_clean")
    )
    joined = src_with_clean.join(tgt_with_clean, on="name_clean", how="inner")
    # Deduplicate: one match per source row
    joined = joined.unique(subset=["vendor_id"])
    t_02 = time.time() - t0
    mem_02 = tracemalloc.get_traced_memory()[1] / 1024 / 1024
    tracemalloc.stop()

    results["TC-02"] = {
        "time_s": round(t_02, 3),
        "memory_mb": round(mem_02, 1),
        "matches": len(joined),
        "rows_tested": len(source),
    }

    # TC-03: Address token overlap (FULL 15k)
    tracemalloc.start()
    t0 = time.time()
    target_addrs_clean = [clean(t["address1"] + " " + t["address2"]) for t in target]
    matches_03 = {"0-59": 0, "60-79": 0, "80-99": 0, "100": 0}
    for row in source:
        src_addr = clean(str(row["hq_addr1"]) + " " + str(row["hq_addr2"]))
        result = rprocess.extractOne(
            src_addr, target_addrs_clean,
            scorer=rfuzz.token_sort_ratio, score_cutoff=0
        )
        pct = int(result[1]) if result else 0
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
        "memory_mb": round(mem_03, 1),
        "score_distribution": matches_03,
        "rows_tested": len(source),
    }

    # TC-04: Combined pipeline (FULL 15k)
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    tracemalloc.start()
    t0 = time.time()

    # Only filter DESTINATION records by date — Pop1's cntrct_cmpl_dt is invalid
    valid_target = target_pl.filter(pl.col("updated") >= cutoff)
    date_filtered = len(target_pl) - len(valid_target)

    valid_target_names = valid_target["vendor_name"].to_list()
    valid_target_clean = [clean(n) for n in valid_target_names]

    matches_04 = []
    for name, vid in zip(source_pl["l3_fmly_nm"].to_list(), source_pl["vendor_id"].to_list()):
        result = rprocess.extractOne(
            clean(name), valid_target_clean,
            scorer=rfuzz.ratio, score_cutoff=85
        )
        if result:
            matches_04.append((vid, valid_target_names[result[2]], result[1] / 100.0))
    t_04 = time.time() - t0
    mem_04 = tracemalloc.get_traced_memory()[1] / 1024 / 1024
    tracemalloc.stop()

    results["TC-04"] = {
        "time_s": round(t_04, 3),
        "memory_mb": round(mem_04, 1),
        "matches": len(matches_04),
        "date_filtered": date_filtered,
        "rows_tested": len(source),
    }

    return results


if __name__ == "__main__":
    with open("benchmarks/results/source.json") as f:
        source = json.load(f)
    with open("benchmarks/results/target.json") as f:
        target = json.load(f)

    results = run(source, target)
    with open("benchmarks/results/option_c.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Option C complete → benchmarks/results/option_c.json")
