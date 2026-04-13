"""
Core matching engine -- ADR Option C aligned.

Uses Polars for all data ops (joins, filtering, expressions).
Uses RapidFuzz process.extract for batch fuzzy matching.
No Python row-level loops for matching -- vectorized throughout.
Address scoring uses RapidFuzz batch ops where possible.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import polars as pl
import numpy as np
from rapidfuzz import fuzz as rfuzz, process as rprocess

from normalize import clean, normalized
from address import score_address_multi_tier


# ---------------------------------------------------------------------------
# Date gate (Polars native)
# ---------------------------------------------------------------------------

_DATE_FORMATS = [
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
]


def apply_date_gate(df: pl.DataFrame, field: str, max_age_years: int) -> pl.DataFrame:
    """Filter records within max_age_years. Pure Polars, no Python loops."""
    cutoff = datetime.now() - timedelta(days=max_age_years * 365)

    for fmt in _DATE_FORMATS:
        try:
            result = df.with_columns(
                pl.col(field).cast(pl.String)
                .str.strptime(pl.Date, fmt, strict=False)
                .alias("_date_gate")
            ).filter(
                pl.col("_date_gate").is_not_null() &
                (pl.col("_date_gate") >= cutoff.date())
            ).drop("_date_gate")

            if result.height > 0:
                return result
        except Exception:
            continue

    # Fallback: string comparison (ISO dates sort correctly)
    return df.filter(pl.col(field).cast(pl.String) >= cutoff.strftime("%Y-%m-%d"))


# ---------------------------------------------------------------------------
# Name matching -- Polars native joins (no Python loops)
# ---------------------------------------------------------------------------

def _normalized_column(df: pl.DataFrame, col: str, alias: str,
                       aliases: dict = None, stopwords: list = None) -> pl.DataFrame:
    """Add a normalized version of a column (clean + aliases + stopwords)."""
    def _norm(val):
        return normalized(val, aliases=aliases, stopwords=stopwords)
    return df.with_columns(
        pl.col(col).cast(pl.String).map_elements(
            _norm, return_dtype=pl.String
        ).alias(alias)
    )


def _clean_column(df: pl.DataFrame, col: str, alias: str) -> pl.DataFrame:
    """Add a cleaned version of a column using normalize.clean().

    Uses the shared normalization function to ensure consistency
    between signal analysis and matching (per README requirement).
    Polars map_elements is used here -- acceptable because this runs
    once per join setup, not per-row in a matching loop.
    """
    return df.with_columns(
        pl.col(col).cast(pl.String).map_elements(
            clean, return_dtype=pl.String
        ).alias(alias)
    )


def match_names_exact(source_df: pl.DataFrame, dest_df: pl.DataFrame,
                      src_field: str, dst_field: str,
                      tiers: list = None,
                      aliases: dict = None,
                      stopwords: list = None) -> pl.DataFrame:
    """Exact name matching via Polars joins. No Python loops.

    Tries tiers in order (default: raw, clean). Deduplicates by tier priority.
    If 'normalized' is in tiers, applies alias replacement + stopword removal
    using the provided aliases/stopwords (requires both to be effective).
    """
    if tiers is None:
        tiers = ["raw", "clean"]

    results = []
    tier_priority = {"raw": 0, "clean": 1, "normalized": 2}

    for tier in tiers:
        if tier == "raw":
            src = source_df.with_columns(pl.col(src_field).cast(pl.String).alias("_match_key"))
            dst = dest_df.with_columns(pl.col(dst_field).cast(pl.String).alias("_match_key"))
        elif tier == "normalized":
            src = _normalized_column(source_df, src_field, "_match_key", aliases, stopwords)
            dst = _normalized_column(dest_df, dst_field, "_match_key", aliases, stopwords)
        else:
            src = _clean_column(source_df, src_field, "_match_key")
            dst = _clean_column(dest_df, dst_field, "_match_key")

        matched = src.join(dst, on="_match_key", how="inner", suffix="_dst")

        if matched.height > 0:
            matched = matched.with_columns(
                pl.lit(tier).alias("match_tier"),
                pl.lit(tier_priority.get(tier, 99)).alias("_tier_priority"),
            )
            results.append(matched)

    if not results:
        return pl.DataFrame()

    combined = pl.concat(results, how="diagonal")

    # Dedup: keep highest priority tier per source record
    combined = (
        combined
        .sort("_tier_priority")
        .unique(subset=[src_field], keep="first")
        .drop([c for c in combined.columns if c.startswith("_")])
    )
    return combined


def match_names_fuzzy(source_df: pl.DataFrame, dest_df: pl.DataFrame,
                      src_field: str, dst_field: str,
                      tiers: list = None,
                      threshold: int = 80,
                      scorer: str = "token_sort_ratio",
                      aliases: dict = None,
                      stopwords: list = None) -> pl.DataFrame:
    """Fuzzy name matching via RapidFuzz cdist (full C++ matrix, no Python loops).

    For each tier, builds match-key columns (same as exact), then uses
    RapidFuzz cdist to compute the full score matrix in C++. Extracts
    the best match per source row above threshold.
    Tries tiers in order; deduplicates by tier priority (earlier tier wins),
    then by highest score within tier.

    Returns matched DataFrame with name_score column (0-100).
    """
    if tiers is None:
        tiers = ["raw", "clean"]

    # Resolve scorer function
    scorer_map = {
        "token_sort_ratio": rfuzz.token_sort_ratio,
        "token_set_ratio": rfuzz.token_set_ratio,
        "ratio": rfuzz.ratio,
        "partial_ratio": rfuzz.partial_ratio,
        "WRatio": rfuzz.WRatio,
    }
    scorer_fn = scorer_map.get(scorer, rfuzz.token_sort_ratio)

    tier_priority = {"raw": 0, "clean": 1, "normalized": 2}
    results = []

    for tier in tiers:
        # Build match keys for this tier (same logic as exact)
        if tier == "raw":
            src = source_df.with_columns(pl.col(src_field).cast(pl.String).alias("_match_key"))
            dst = dest_df.with_columns(pl.col(dst_field).cast(pl.String).alias("_match_key"))
        elif tier == "normalized":
            src = _normalized_column(source_df, src_field, "_match_key", aliases, stopwords)
            dst = _normalized_column(dest_df, dst_field, "_match_key", aliases, stopwords)
        else:  # clean
            src = _clean_column(source_df, src_field, "_match_key")
            dst = _clean_column(dest_df, dst_field, "_match_key")

        # Build key lists for cdist
        src_keys = [str(k) if k is not None else "" for k in src["_match_key"].to_list()]
        dst_keys = [str(k) if k is not None else "" for k in dst["_match_key"].to_list()]
        if not dst_keys or not src_keys:
            continue

        # Compute full score matrix in C++ (no Python loops)
        # workers=-1 uses all available cores
        score_matrix = rprocess.cdist(
            src_keys, dst_keys,
            scorer=scorer_fn, score_cutoff=threshold,
            dtype=np.float32, workers=-1,
        )

        # Extract best match per source row
        best_dst_idxs = score_matrix.argmax(axis=1)
        best_scores = score_matrix[np.arange(len(src_keys)), best_dst_idxs]

        # Filter to rows that met the threshold (cdist sets below-threshold to 0)
        mask = best_scores >= threshold
        if not mask.any():
            continue

        src_idxs = np.where(mask)[0].tolist()
        dst_idxs = best_dst_idxs[mask].tolist()
        scores = best_scores[mask].tolist()

        matched_src = src[src_idxs].drop("_match_key")
        matched_dst = dst[dst_idxs].drop("_match_key")

        # Rename dst columns with _dst suffix to avoid collision
        dst_renames = {}
        for col in matched_dst.columns:
            if col in matched_src.columns:
                dst_renames[col] = col + "_dst"
        if dst_renames:
            matched_dst = matched_dst.rename(dst_renames)

        matched = pl.concat([matched_src, matched_dst], how="horizontal")
        matched = matched.with_columns(
            pl.Series("name_score", scores),
            pl.lit(tier).alias("match_tier"),
            pl.lit(tier_priority.get(tier, 99)).alias("_tier_priority"),
        )

        results.append(matched)

    if not results:
        return pl.DataFrame()

    combined = pl.concat(results, how="diagonal")

    # Dedup: keep best tier per source record, then highest score within tier
    combined = (
        combined
        .sort(["_tier_priority", "name_score"], descending=[False, True])
        .unique(subset=[src_field], keep="first")
        .drop([c for c in combined.columns if c.startswith("_")])
    )
    return combined


# ---------------------------------------------------------------------------
# Address scoring -- RapidFuzz batch ops
# ---------------------------------------------------------------------------

def score_addresses_batch(matched_df: pl.DataFrame,
                          src_a1: str, src_a2: str,
                          dst_a1: str, dst_a2: str,
                          parser: str = "auto",
                          aliases: dict = None,
                          stopwords: list = None) -> pl.DataFrame:
    """Score address pairs using Phase 3 address module.

    Uses score_address_multi_tier which:
    - Builds variants and cross-compares (merged, a1-a1, a1-a2, a2-a1, a2-a2)
    - Parses via libpostal or built-in tokenizer for street name extraction
    - Applies all normalization tiers (Raw -> Clean -> Normalized for addresses)
    - Weights street name match at 60/40

    Iterates over matched pairs (post-join, not N×M).
    """
    src_a1_vals = matched_df[src_a1].to_list()
    src_a2_vals = matched_df[src_a2].to_list()
    dst_a1_vals = matched_df[dst_a1].to_list()
    dst_a2_vals = matched_df[dst_a2].to_list()

    scores = []
    street_matches = []
    comparisons = []
    tiers_used = []

    for s1, s2, d1, d2 in zip(src_a1_vals, src_a2_vals, dst_a1_vals, dst_a2_vals):
        result = score_address_multi_tier(
            str(s1 or ""), str(s2 or ""),
            str(d1 or ""), str(d2 or ""),
            tiers=["raw", "clean", "normalized"],
            parser=parser,
            aliases=aliases,
            stopwords=stopwords,
        )
        scores.append(result["best_score"])
        street_matches.append(result.get("street_match", False))
        comparisons.append(result.get("best_comparison", ""))
        tiers_used.append(result.get("tier_used", ""))

    return matched_df.with_columns(
        pl.Series("addr_score", scores),
        pl.Series("addr_street_match", street_matches),
        pl.Series("addr_comparison", comparisons),
        pl.Series("addr_tier", tiers_used),
    )


# ---------------------------------------------------------------------------
# Single matching step
# ---------------------------------------------------------------------------

def run_matching_step(source_df: pl.DataFrame, dest_df: pl.DataFrame,
                      step_config: dict,
                      aliases: dict = None, stopwords: list = None) -> pl.DataFrame:
    """Execute one matching step from the recipe."""

    # Date gate on destination
    if "date_gate" in step_config:
        dg = step_config["date_gate"]
        if dg.get("applies_to") in ("destination", "both"):
            dest_df = apply_date_gate(dest_df, dg["field"], dg["max_age_years"])
        if dest_df.height == 0:
            return pl.DataFrame()

    # Name matching
    mf = step_config["match_fields"][0]
    method = mf.get("method", "exact")

    if method == "fuzzy":
        matched = match_names_fuzzy(
            source_df, dest_df,
            mf["source"], mf["destination"],
            mf.get("tiers", ["raw", "clean"]),
            threshold=mf.get("threshold", 80),
            scorer=mf.get("scorer", "token_sort_ratio"),
            aliases=aliases,
            stopwords=stopwords,
        )
    else:
        matched = match_names_exact(
            source_df, dest_df,
            mf["source"], mf["destination"],
            mf.get("tiers", ["raw", "clean"]),
            aliases=aliases,
            stopwords=stopwords,
        )

    if matched.height == 0:
        return pl.DataFrame()

    # Address scoring (if configured)
    if "address_support" in step_config:
        ac = step_config["address_support"]
        src_a1, src_a2 = ac["source"][0], ac["source"][1] if len(ac["source"]) > 1 else ac["source"][0]
        dst_a1 = ac["destination"][0]
        dst_a2 = ac["destination"][1] if len(ac["destination"]) > 1 else ac["destination"][0]

        # Handle suffixed column names from join
        if dst_a1 not in matched.columns and dst_a1 + "_dst" in matched.columns:
            dst_a1 = dst_a1 + "_dst"
        if dst_a2 not in matched.columns and dst_a2 + "_dst" in matched.columns:
            dst_a2 = dst_a2 + "_dst"

        # Score using Phase 3 address module (multi-tier, street weighting, cross-compare)
        matched = score_addresses_batch(
            matched, src_a1, src_a2, dst_a1, dst_a2,
            parser=ac.get("parser", "auto"),
        )

    # Add step metadata
    matched = matched.with_columns(pl.lit(step_config["name"]).alias("match_step"))

    # Inherit fields
    for inherit_cfg in step_config.get("inherit", []):
        src_col = inherit_cfg["source"]
        as_col = inherit_cfg["as"]
        if src_col in matched.columns:
            matched = matched.rename({src_col: as_col})
        elif src_col + "_dst" in matched.columns:
            matched = matched.rename({src_col + "_dst": as_col})

    return matched


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def run_pipeline(recipe: dict, base_dir: str = ".") -> dict:
    """Run the complete matching pipeline from a recipe.

    Returns dict with 'matched', 'unmatched' DataFrames and 'stats'.
    """
    from recipe import load_source, filter_population, build_filter_expr

    # Load sources
    sources = {}
    for name, cfg in recipe["sources"].items():
        sources[name] = load_source(cfg, base_dir)

    # Pre-validate filter fields before building populations
    filter_errors = []
    for pop_name, pop_cfg in recipe["populations"].items():
        src_name = pop_cfg.get("source", "")
        if src_name not in sources:
            continue
        src_cols = set(sources[src_name].columns)
        for cond in pop_cfg.get("filter", []):
            if "field" in cond and cond["field"] not in src_cols:
                available = ", ".join(sorted(src_cols)[:10])
                filter_errors.append(
                    f'Population "{pop_name}" filter field "{cond["field"]}" '
                    f"not found. Available: {available}"
                )
    if filter_errors:
        from recipe import RecipeValidationError
        import sys
        for e in filter_errors:
            print(f"[ERROR] {e}", file=sys.stderr)
        raise RecipeValidationError(
            f"Recipe has {len(filter_errors)} filter field error(s). Fix recipe config and retry."
        )

    # Build populations
    populations = {}
    for pop_name, pop_cfg in recipe["populations"].items():
        src_name = pop_cfg["source"]
        src_df = sources[src_name]

        if "filter" in pop_cfg and pop_cfg["filter"]:
            filtered = filter_population(src_df, pop_cfg)
            populations[pop_name] = {"config": pop_cfg, "df": filtered, "source": src_name}
        else:
            # Remainder -- computed after other pops
            populations[pop_name] = {"config": pop_cfg, "df": None, "source": src_name}

    # Compute remainder populations (exclude Pop1 + Garbage from same source)
    for pop_name, pop_data in populations.items():
        if pop_data["df"] is not None:
            continue

        src_df = sources[pop_data["source"]]
        remainder = src_df

        for other_name, other_data in populations.items():
            if other_name == pop_name or other_data["source"] != pop_data["source"]:
                continue
            other_cfg = other_data["config"]
            if "filter" in other_cfg and other_cfg["filter"]:
                remainder = remainder.filter(~build_filter_expr(other_cfg["filter"]))

        # Also exclude garbage populations
        for garb_name, garb_cfg in recipe["populations"].items():
            if garb_name == pop_name:
                continue
            if garb_cfg.get("action") == "exclude" and "filter" in garb_cfg and garb_cfg["filter"]:
                remainder = remainder.filter(~build_filter_expr(garb_cfg["filter"]))

        pop_data["df"] = remainder

    # Semantic field validation — runs every time, not just dry-run
    from recipe import validate_fields, RecipeValidationError
    val_errors, val_warnings = validate_fields(recipe, sources, populations)
    for w in val_warnings:
        import sys
        print(f"[WARN] {w}", file=sys.stderr)
    if val_errors:
        import sys
        for e in val_errors:
            print(f"[ERROR] {e}", file=sys.stderr)
        raise RecipeValidationError(
            f"Recipe has {len(val_errors)} field error(s). Fix recipe config and retry."
        )

    # Load normalization config (aliases/stopwords for normalized tier)
    norm_cfg = recipe.get("normalization", {})
    aliases = None
    stopwords = None
    if norm_cfg.get("aliases"):
        aliases_path = Path(norm_cfg["aliases"]) if Path(norm_cfg["aliases"]).is_absolute() else Path(norm_cfg["aliases"])
        # Try relative to cwd first, then relative to base_dir
        if not aliases_path.exists():
            aliases_path = Path(base_dir) / norm_cfg["aliases"]
        if aliases_path.exists():
            aliases = json.loads(aliases_path.read_text())
    if norm_cfg.get("stopwords"):
        sw_path = Path(norm_cfg["stopwords"]) if Path(norm_cfg["stopwords"]).is_absolute() else Path(norm_cfg["stopwords"])
        if not sw_path.exists():
            sw_path = Path(base_dir) / norm_cfg["stopwords"]
        if sw_path.exists():
            sw_data = json.loads(sw_path.read_text())
            # Flatten stopwords if categorized by type (name/address)
            if isinstance(sw_data, dict):
                stopwords = [w for words in sw_data.values() for w in words]
            else:
                stopwords = sw_data

    # Run matching steps
    match_mode = recipe.get("output", {}).get("match_mode", "best_match")
    all_matched = []
    matched_source_keys = set()

    # Determine the source ID field for unmatched tracking
    # Use vendor_id or the match field as fallback
    pop1_name = recipe["steps"][0]["source"]
    src_match_field = recipe["steps"][0]["match_fields"][0]["source"]
    pop1_df_check = populations.get(pop1_name, {}).get("df", pl.DataFrame())
    # Prefer vendor_id if available (more unique than names)
    track_field = "vendor_id" if "vendor_id" in pop1_df_check.columns else src_match_field

    for step_idx, step in enumerate(recipe["steps"]):
        src_pop = step["source"]
        dst_pop = step["destination"]

        src_df = populations.get(src_pop, {}).get("df")
        if src_df is None or src_df.height == 0:
            continue

        dst_df = populations.get(dst_pop, {}).get("df")
        if dst_df is None:
            dst_df = sources.get(dst_pop)
        if dst_df is None or dst_df.height == 0:
            continue

        matched = run_matching_step(src_df, dst_df, step,
                                       aliases=aliases, stopwords=stopwords)

        if matched.height > 0:
            # Tag step order for multi-match resolution
            matched = matched.with_columns(pl.lit(step_idx).alias("_step_order"))
            all_matched.append(matched)

            if track_field in matched.columns:
                matched_source_keys.update(matched[track_field].to_list())

    # Combine and resolve
    if all_matched:
        combined = pl.concat(all_matched, how="diagonal")

        if match_mode == "best_match":
            src_field = recipe["steps"][0]["match_fields"][0]["source"]
            # Sort: prefer earlier step, then higher name score, then higher address score
            sort_cols = ["_step_order"]
            sort_desc = [False]
            if "name_score" in combined.columns:
                sort_cols.append("name_score")
                sort_desc.append(True)
            if "addr_score" in combined.columns:
                sort_cols.append("addr_score")
                sort_desc.append(True)
            combined = combined.sort(sort_cols, descending=sort_desc)
            combined = combined.unique(subset=[src_field], keep="first")

        # Exact matches get name_score=100 (fuzzy steps already have scores)
        if "name_score" in combined.columns:
            combined = combined.with_columns(
                pl.col("name_score").fill_null(100.0).alias("name_score")
            )

        combined = combined.drop([c for c in combined.columns if c.startswith("_")])
    else:
        combined = pl.DataFrame()

    # Unmatched
    pop1_name = recipe["steps"][0]["source"]
    pop1_df = populations.get(pop1_name, {}).get("df", pl.DataFrame())
    src_field = recipe["steps"][0]["match_fields"][0]["source"]

    if pop1_df.height > 0 and track_field in pop1_df.columns:
        unmatched = pop1_df.filter(~pl.col(track_field).is_in(list(matched_source_keys)))
    else:
        unmatched = pl.DataFrame()

    return {
        "matched": combined,
        "unmatched": unmatched,
        "populations": {k: v["df"] for k, v in populations.items() if v["df"] is not None},
        "stats": {
            "total_source": pop1_df.height,
            "matched_count": combined.height,
            "unmatched_count": unmatched.height,
        },
    }
