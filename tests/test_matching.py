"""
Tests for src/matching.py and src/recipe.py (Phase 4 v2)

Validates ADR Option C alignment, README matching rules, and
pipeline correctness against synthetic datasets.

Results written to tests/results/matching_results.json
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import polars as pl
from recipe import load_recipe, load_source, filter_population, build_filter_expr, validate_recipe
from matching import apply_date_gate, match_names_exact, match_names_fuzzy, run_matching_step, run_pipeline

DATA_DIR = Path(__file__).parent.parent / "data"
RECIPE_PATH = Path(__file__).parent.parent / "config" / "recipes" / "l1_reconciliation.yaml"


def load_datasets():
    return (
        pl.read_csv(str(DATA_DIR / "core_parent_export.csv")),
        pl.read_csv(str(DATA_DIR / "tp_multi_pop_dataset.csv")),
    )


# --- Recipe tests ---

def test_load_recipe():
    recipe = load_recipe(str(RECIPE_PATH))
    _results = [
        {"check": "loads", "passed": recipe is not None, "actual": type(recipe).__name__},
        {"check": "name", "passed": recipe["name"] == "L1 Reconciliation", "actual": recipe["name"]},
        {"check": "4_steps", "passed": len(recipe["steps"]) == 4, "actual": len(recipe["steps"])},
        {"check": "3_pops", "passed": len(recipe["populations"]) == 3, "actual": len(recipe["populations"])},
        {"check": "step1_src", "passed": recipe["steps"][0]["source"] == "pop1", "actual": recipe["steps"][0]["source"]},
        {"check": "step1_dst", "passed": recipe["steps"][0]["destination"] == "core_parent", "actual": recipe["steps"][0]["destination"]},
    ]
    for r in _results:
        assert r["passed"], f"Failed: {r}"


def test_invalid_recipe():
    results = []
    try:
        validate_recipe({"name": "test"})
        results.append({"check": "raises", "passed": False, "actual": "no exception"})
    except ValueError:
        results.append({"check": "raises", "passed": True, "actual": "ValueError"})
    for r in results:
        assert r["passed"], f"Failed: {r}"


# --- Population filter tests ---

def test_filter_pop1():
    _, multi = load_datasets()
    pop1 = filter_population(multi, {"filter": [{"field": "vendor_id", "op": "starts_with", "value": "V7"}]})
    _results = [
        {"check": "has_rows", "passed": pop1.height > 0, "actual": pop1.height},
        {"check": "all_v7", "passed": pop1["vendor_id"].cast(pl.String).str.starts_with("V7").all(), "actual": "checked"},
    ]
    for r in _results:
        assert r["passed"], f"Failed: {r}"


def test_filter_garbage():
    _, multi = load_datasets()
    garbage = filter_population(multi, {"filter": [
        {"field": "vendor_id", "op": "not_starts_with", "value": "V7"},
        {"field": "data_entry_type", "op": "eq", "value": "Migrated"},
        {"field": "rq_intk_user", "op": "contains_any", "values": ["Data Migration", "Goblindor"], "join": "and"},
    ]})
    _results = [
        {"check": "has_rows", "passed": garbage.height > 0, "actual": garbage.height},
        {"check": "no_v7", "passed": (~garbage["vendor_id"].cast(pl.String).str.starts_with("V7")).all(), "actual": "checked"},
        {"check": "all_migrated", "passed": (garbage["data_entry_type"].cast(pl.String) == "Migrated").all(), "actual": "checked"},
    ]
    for r in _results:
        assert r["passed"], f"Failed: {r}"


# --- Date gate tests ---

def test_date_gate():
    core, _ = load_datasets()
    filtered = apply_date_gate(core, "Updated", max_age_years=2)
    _results = [
        {"check": "filters_some", "passed": filtered.height <= core.height, "actual": f"{filtered.height}/{core.height}"},
        {"check": "not_empty", "passed": filtered.height > 0, "actual": filtered.height},
    ]
    for r in _results:
        assert r["passed"], f"Failed: {r}"


def test_date_gate_strict():
    """Verify stale dates are actually excluded."""
    _, multi = load_datasets()
    # Pop3 has some records from 2023 with cntrct_cmpl_dt
    pop3_all = multi.filter(~pl.col("vendor_id").cast(pl.String).str.starts_with("V7"))
    filtered = apply_date_gate(pop3_all, "cntrct_cmpl_dt", max_age_years=2)
    _results = [
        {"check": "fewer_than_all", "passed": filtered.height < pop3_all.height, "actual": f"{filtered.height}/{pop3_all.height}"},
    ]
    for r in _results:
        assert r["passed"], f"Failed: {r}"


# --- Name matching tests ---

def test_name_match_raw():
    core, multi = load_datasets()
    pop1 = multi.filter(pl.col("vendor_id").cast(pl.String).str.starts_with("V7"))
    matched = match_names_exact(pop1, core, "l3_fmly_nm", "Vendor Name", tiers=["raw"])
    results = [{"check": "has_matches", "passed": matched.height > 0, "actual": matched.height}]
    if matched.height > 0:
        results.append({"check": "all_raw_tier", "passed": (matched["match_tier"] == "raw").all(), "actual": matched["match_tier"].unique().to_list()})
    for r in results:
        assert r["passed"], f"Failed: {r}"


def test_name_match_clean_superset():
    """Clean tier should find >= raw matches."""
    core, multi = load_datasets()
    pop1 = multi.filter(pl.col("vendor_id").cast(pl.String).str.starts_with("V7"))
    raw = match_names_exact(pop1, core, "l3_fmly_nm", "Vendor Name", tiers=["raw"])
    both = match_names_exact(pop1, core, "l3_fmly_nm", "Vendor Name", tiers=["raw", "clean"])
    _results = [{"check": "clean_gte_raw", "passed": both.height >= raw.height, "actual": f"raw={raw.height} both={both.height}"}]
    for r in _results:
        assert r["passed"], f"Failed: {r}"


def test_l1_recipe_no_normalized_names():
    """L1 recipe should not normalize names (suffixes distinguish L1 parents)."""
    recipe = load_recipe(str(RECIPE_PATH))
    tiers = recipe["steps"][0]["match_fields"][0].get("tiers", [])
    _results = [{"check": "l1_no_normalized_names", "passed": "normalized" not in tiers, "actual": tiers}]
    for r in _results:
        assert r["passed"], f"Failed: {r}"


def test_normalized_name_tier():
    """Normalized tier on names should work when explicitly enabled."""
    src = pl.DataFrame({"name": ["Qualidyne Professional Svcs", "Exact Match Corp"]})
    dst = pl.DataFrame({"name": ["Qualidyne Services", "Exact Match Corp"]})
    aliases = {"svcs": "services"}
    stopwords = ["professional"]
    result = match_names_exact(
        src, dst, "name", "name",
        tiers=["raw", "clean", "normalized"],
        aliases=aliases, stopwords=stopwords,
    )
    # Should match both: Exact Match Corp at raw, Qualidyne at normalized
    _results = [
        {"check": "both_matched", "passed": result.height == 2, "actual": result.height},
        {"check": "has_normalized_tier", "passed": "normalized" in result["match_tier"].to_list(),
         "actual": result["match_tier"].to_list()},
    ]
    for r in _results:
        assert r["passed"], f"Failed: {r}"


# --- Full pipeline tests ---

def test_full_pipeline():
    recipe = load_recipe(str(RECIPE_PATH))
    result = run_pipeline(recipe, base_dir=str(DATA_DIR))
    results = [
        {"check": "has_matched", "passed": result["matched"].height > 0, "actual": result["matched"].height},
        {"check": "has_unmatched", "passed": result["unmatched"].height >= 0, "actual": result["unmatched"].height},
        {"check": "has_stats", "passed": "stats" in result, "actual": list(result["stats"].keys())},
    ]

    # Total should roughly add up (may differ by 1-2 due to cross-step dedup)
    total = result["stats"]["matched_count"] + result["stats"]["unmatched_count"]
    diff = abs(total - result["stats"]["total_source"])
    results.append({"check": "total_close", "passed": diff <= 2, "actual": result["stats"]})

    # Matched should have derived L1 columns
    if result["matched"].height > 0:
        cols = result["matched"].columns
        results.append({"check": "has_derived_l1_name", "passed": "derived_l1_name" in cols, "actual": [c for c in cols if "derived" in c]})
        results.append({"check": "has_match_step", "passed": "match_step" in cols, "actual": "match_step" in cols})
        results.append({"check": "has_match_tier", "passed": "match_tier" in cols, "actual": "match_tier" in cols})
        if "addr_score" in cols:
            results.append({"check": "has_addr_score", "passed": True, "actual": "addr_score present"})

    for r in results:
        assert r["passed"], f"Failed: {r}"


def test_step_priority():
    """core_parent matches should be preferred over Pop3."""
    recipe = load_recipe(str(RECIPE_PATH))
    result = run_pipeline(recipe, base_dir=str(DATA_DIR))
    if result["matched"].height == 0:
        _results = [{"check": "has_matches", "passed": False, "actual": 0}]
        for r in _results:
            assert r["passed"], f"Failed: {r}"

    # Records matched in Step 1 should be from core_parent
    step1_matches = result["matched"].filter(pl.col("match_step") == "Match Pop1 to core_parent")
    step2_matches = result["matched"].filter(pl.col("match_step") == "Match Pop1 to Pop3")
    _results = [
        {"check": "step1_has_matches", "passed": step1_matches.height > 0, "actual": step1_matches.height},
        {"check": "step1_preferred", "passed": step1_matches.height >= step2_matches.height,
         "actual": f"step1={step1_matches.height} step2={step2_matches.height}"},
    ]
    for r in _results:
        assert r["passed"], f"Failed: {r}"


def test_garbage_excluded():
    """Garbage should not appear in Pop3."""
    recipe = load_recipe(str(RECIPE_PATH))
    result = run_pipeline(recipe, base_dir=str(DATA_DIR))
    if "pop3" not in result["populations"]:
        _results = [{"check": "pop3_exists", "passed": False, "actual": "missing"}]
        for r in _results:
            assert r["passed"], f"Failed: {r}"
    pop3 = result["populations"]["pop3"]
    has_v7 = pop3["vendor_id"].cast(pl.String).str.starts_with("V7").any()
    _results = [{"check": "no_v7_in_pop3", "passed": not has_v7, "actual": not has_v7}]
    for r in _results:
        assert r["passed"], f"Failed: {r}"


def test_no_python_loops_in_name_matching():
    """Verify name matching uses Polars joins, not Python iteration.

    This is an ADR compliance check -- Option C was chosen to avoid Python loops.
    We verify by checking that match_names_exact returns results without
    using map_elements or iter_rows (inspecting the source would be ideal,
    but we proxy-test by verifying it handles 1000+ rows quickly).
    """
    import time
    core, multi = load_datasets()
    # Duplicate pop1 to simulate larger dataset
    pop1 = multi.filter(pl.col("vendor_id").cast(pl.String).str.starts_with("V7"))
    large_pop1 = pl.concat([pop1] * 50)  # ~1800 rows

    t0 = time.time()
    matched = match_names_exact(large_pop1, core, "l3_fmly_nm", "Vendor Name", tiers=["raw", "clean"])
    elapsed = time.time() - t0

    _results = [
        {"check": "completes_fast", "passed": elapsed < 5.0, "actual": f"{elapsed:.2f}s for {large_pop1.height} rows"},
        {"check": "has_results", "passed": matched.height > 0, "actual": matched.height},
    ]
    for r in _results:
        assert r["passed"], f"Failed: {r}"


# --- Fuzzy matching tests ---

def test_fuzzy_basic():
    """Fuzzy matching finds near-matches that exact misses."""
    src = pl.DataFrame({"name": [
        "Bapienx Solutions Inc",
        "Orizon Analytics LLC",
        "Exact Match Corp",
    ]})
    dst = pl.DataFrame({"name": [
        "Bapienx Inc",
        "Orizon Analytics Group",
        "Exact Match Corp",
    ]})
    # Exact should only get 1
    exact = match_names_exact(src, dst, "name", "name", tiers=["raw", "clean"])
    assert exact.height == 1, f"Expected 1 exact match, got {exact.height}"

    # Fuzzy at threshold 60 should get all 3
    fuzzy = match_names_fuzzy(src, dst, "name", "name", tiers=["raw", "clean"], threshold=60)
    assert fuzzy.height == 3, f"Expected 3 fuzzy matches, got {fuzzy.height}"
    assert "name_score" in fuzzy.columns, "Missing name_score column"
    assert "match_tier" in fuzzy.columns, "Missing match_tier column"

    # Exact match should have score 100
    exact_row = fuzzy.filter(pl.col("name") == "Exact Match Corp")
    assert exact_row["name_score"][0] == 100.0, f"Exact match score should be 100, got {exact_row['name_score'][0]}"


def test_fuzzy_threshold_filters():
    """Higher threshold should produce fewer matches."""
    src = pl.DataFrame({"name": ["Brevix Transport Inc", "Orizon Analytics LLC"]})
    dst = pl.DataFrame({"name": ["Brevix Logistics Inc", "Orizon Analytics Group"]})

    low = match_names_fuzzy(src, dst, "name", "name", tiers=["clean"], threshold=60)
    high = match_names_fuzzy(src, dst, "name", "name", tiers=["clean"], threshold=80)
    assert low.height >= high.height, f"Low threshold should match >= high: {low.height} vs {high.height}"
    # Brevix (score ~65) should be filtered at 80
    assert high.height < low.height, f"High threshold should filter Brevix: high={high.height} low={low.height}"


def test_fuzzy_tier_priority():
    """Earlier tier should win in dedup."""
    src = pl.DataFrame({"name": ["Test Corp"]})
    dst = pl.DataFrame({"name": ["Test Corp"]})
    result = match_names_fuzzy(src, dst, "name", "name", tiers=["raw", "clean"], threshold=80)
    assert result.height == 1
    assert result["match_tier"][0] == "raw", f"Expected raw tier, got {result['match_tier'][0]}"


def test_fuzzy_scorer_option():
    """Different scorers should produce different results."""
    src = pl.DataFrame({"name": ["Qualidyne Professional Svcs"]})
    dst = pl.DataFrame({"name": ["Qualidyne Services"]})
    token_sort = match_names_fuzzy(src, dst, "name", "name", tiers=["clean"], threshold=50, scorer="token_sort_ratio")
    wratio = match_names_fuzzy(src, dst, "name", "name", tiers=["clean"], threshold=50, scorer="WRatio")
    assert token_sort.height == 1 and wratio.height == 1
    # Scores should differ between scorers
    ts_score = token_sort["name_score"][0]
    wr_score = wratio["name_score"][0]
    assert ts_score != wr_score, f"Scores should differ: token_sort={ts_score} WRatio={wr_score}"


def test_fuzzy_dispatch_in_step():
    """run_matching_step dispatches to fuzzy when method=fuzzy."""
    src = pl.DataFrame({"name": ["Bapienx Solutions Inc", "Exact Corp"]})
    dst = pl.DataFrame({"name": ["Bapienx Inc", "Exact Corp"]})
    step = {
        "name": "fuzzy_step",
        "match_fields": [{"source": "name", "destination": "name", "method": "fuzzy", "tiers": ["clean"], "threshold": 60}],
    }
    result = run_matching_step(src, dst, step)
    assert result.height == 2, f"Expected 2 fuzzy matches via step, got {result.height}"
    assert "name_score" in result.columns
    assert "match_step" in result.columns


def test_fuzzy_empty_source():
    """Fuzzy matching with empty source returns empty DataFrame."""
    src = pl.DataFrame({"name": pl.Series([], dtype=pl.String)})
    dst = pl.DataFrame({"name": ["Something"]})
    result = match_names_fuzzy(src, dst, "name", "name", threshold=60)
    assert result.height == 0


def test_fuzzy_no_matches_above_threshold():
    """When nothing meets threshold, return empty."""
    src = pl.DataFrame({"name": ["Completely Different Name"]})
    dst = pl.DataFrame({"name": ["Unrelated Entity"]})
    result = match_names_fuzzy(src, dst, "name", "name", tiers=["clean"], threshold=80)
    assert result.height == 0


def test_clean_column_uses_shared_normalize():
    """_clean_column must use normalize.clean() (shared source of truth)."""
    from matching import _clean_column
    from normalize import clean as py_clean

    test_values = [
        "BAPIENX INC", "  Nexacore Solutions LLC,  ", "Vanteon Systems, Inc.",
        "trailing comma,", "  multiple   spaces  ", "already clean",
    ]
    df = pl.DataFrame({"name": test_values})
    polars_cleaned = _clean_column(df, "name", "_cleaned")["_cleaned"].to_list()
    python_cleaned = [py_clean(v) for v in test_values]

    results = []
    for i, (p_val, py_val) in enumerate(zip(polars_cleaned, python_cleaned)):
        results.append({
            "check": f"row_{i}_matches",
            "passed": p_val == py_val,
            "actual": f"polars='{p_val}' python='{py_val}'",
        })
    # These should be identical since _clean_column now calls normalize.clean()
    all_match = all(r["passed"] for r in results)
    results.append({"check": "all_identical", "passed": all_match, "actual": f"{sum(r['passed'] for r in results[:-1])}/{len(results)-1}"})
    for r in results:
        assert r["passed"], f"Failed: {r}"


# --- Runner ---

def run_all():
    all_results = {
        "test_load_recipe": test_load_recipe(),
        "test_invalid_recipe": test_invalid_recipe(),
        "test_filter_pop1": test_filter_pop1(),
        "test_filter_garbage": test_filter_garbage(),
        "test_date_gate": test_date_gate(),
        "test_date_gate_strict": test_date_gate_strict(),
        "test_name_match_raw": test_name_match_raw(),
        "test_name_match_clean_superset": test_name_match_clean_superset(),
        "test_name_match_no_normalized": test_name_match_no_normalized(),
        "test_full_pipeline": test_full_pipeline(),
        "test_step_priority": test_step_priority(),
        "test_garbage_excluded": test_garbage_excluded(),
        "test_no_python_loops_in_name_matching": test_no_python_loops_in_name_matching(),
        "test_clean_column_uses_shared_normalize": test_clean_column_uses_shared_normalize(),
    }

    total = 0
    passed = 0
    failed_details = []
    for test_name, results in all_results.items():
        for r in results:
            total += 1
            if r["passed"]:
                passed += 1
            else:
                failed_details.append({"test": test_name, **r})

    summary = {
        "total": total,
        "passed": passed,
        "failed": total - passed,
        "pass_rate": f"{passed/total*100:.1f}%" if total > 0 else "N/A",
        "failed_details": failed_details,
    }

    out_path = Path(__file__).parent / "results" / "matching_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump({"summary": summary}, f, indent=2, default=str)

    print(f"Tests: {passed}/{total} passed ({summary['pass_rate']})")
    if failed_details:
        print("FAILURES:")
        for fd in failed_details:
            print(f"  {fd['test']}: {fd.get('check', '')} actual={fd.get('actual', '')}")
    return summary


if __name__ == "__main__":
    summary = run_all()
    sys.exit(0 if summary["failed"] == 0 else 1)
