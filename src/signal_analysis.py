"""
Signal Analysis module for the relational matching framework.

Profiles source data to bootstrap normalization config:
- Top N tokens per column at Raw and Clean tiers
- Auto-detect column type (name, address, date, ID)
- Suggested stopwords from frequency distribution
- Suggested alias groups from variant detection
- Unicode profile per column
- Data quality summary

Uses normalize.py for all transformations. Single source of truth.
"""

import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

import polars as pl

from normalize import clean, profile_column as unicode_profile_column, _load_ranges


# ---------------------------------------------------------------------------
# Column type detection
# ---------------------------------------------------------------------------

# Patterns for auto-detection
_DATE_PATTERNS = [
    re.compile(r'^\d{4}-\d{2}-\d{2}'),           # ISO date
    re.compile(r'^\d{1,2}/\d{1,2}/\d{2,4}'),     # US date
    re.compile(r'^\d{1,2}-\d{1,2}-\d{2,4}'),     # Dash date
]
_ID_PATTERN = re.compile(r'^[A-Za-z]?\d{3,}$')    # Alphanumeric ID-like
_NAME_SUFFIXES = {"inc", "llc", "ltd", "corp", "co", "group", "pty", "gmbh", "sa", "ag"}
_ADDR_TOKENS = {"street", "st", "avenue", "ave", "blvd", "boulevard", "drive", "dr",
                "road", "rd", "lane", "ln", "suite", "ste", "floor", "fl", "pkwy"}


def select_columns(df: pl.DataFrame, columns_arg: str | None) -> tuple[list[str], str]:
    """Resolve --columns arg to a column list. Returns (columns, mode_message)."""
    if columns_arg and columns_arg.lower() == "auto":
        columns = [c for c in df.columns if detect_column_type(df[c]) in ("name", "address")]
        if not columns:
            columns = [c for c in df.columns if df[c].dtype == pl.String]
        return columns, f"Auto-selected columns: {', '.join(columns)}"
    elif columns_arg:
        columns = [c.strip() for c in columns_arg.split(",")]
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(
                f"Columns not found: {', '.join(missing)}\n"
                f"Available: {', '.join(df.columns)}"
            )
        return columns, ""
    else:
        columns = [c for c in df.columns if df[c].dtype == pl.String]
        return columns, f"Analyzing all string columns: {', '.join(columns)}"


def detect_column_type(series: pl.Series) -> str:
    """Auto-detect column type based on value patterns.

    Returns: 'name', 'address', 'date', 'id', or 'freetext'
    """
    sample = series.drop_nulls().head(100).to_list()
    if not sample:
        return "freetext"

    date_hits = 0
    id_hits = 0
    name_hits = 0
    addr_hits = 0

    for val in sample:
        s = str(val).strip()
        if not s:
            continue

        # Date check
        for pat in _DATE_PATTERNS:
            if pat.match(s):
                date_hits += 1
                break

        # ID check
        if _ID_PATTERN.match(s):
            id_hits += 1

        # Name/address check via tokens
        tokens = {t.lower().rstrip(".,") for t in s.split()}
        if tokens & _NAME_SUFFIXES:
            name_hits += 1
        if tokens & _ADDR_TOKENS:
            addr_hits += 1

    total = len(sample)
    threshold = 0.3  # 30% of sample must match

    if date_hits / total > threshold:
        return "date"
    if id_hits / total > threshold:
        return "id"
    if addr_hits / total > threshold:
        return "address"
    if name_hits / total > threshold:
        return "name"

    # Heuristic: high uniqueness + short values = ID
    unique_ratio = series.n_unique() / series.len() if series.len() > 0 else 0
    if unique_ratio > 0.9:
        return "id"

    return "freetext"


# ---------------------------------------------------------------------------
# Token analysis
# ---------------------------------------------------------------------------

def top_tokens(series: pl.Series, tier: str = "raw", n: int = 50) -> list[tuple[str, int]]:
    """Extract top N tokens from a column at a given normalization tier.

    Args:
        series: Polars Series of string values
        tier: 'raw' or 'clean'
        n: Number of top tokens to return

    Returns:
        List of (token, count) tuples sorted by frequency descending
    """
    if tier not in ("raw", "clean"):
        raise ValueError(f"top_tokens only supports 'raw' or 'clean' tiers, got '{tier}'")

    s = series.drop_nulls().cast(pl.String)
    if tier == "clean":
        s = s.str.to_lowercase().str.replace_all(r'[,.;:]', '')
    # Split, explode, filter empty, count
    df = s.str.split(" ").explode().to_frame("tok")
    df = df.filter(pl.col("tok") != "")
    counts = df.group_by("tok").len().sort("len", descending=True).head(n)
    return [(row[0], row[1]) for row in counts.iter_rows()]


# ---------------------------------------------------------------------------
# Stopword suggestion
# ---------------------------------------------------------------------------

def suggest_stopwords(series: pl.Series, col_type: str = "name",
                      threshold: float = 0.15, n: int = 20) -> list:
    """Suggest stopwords based on token frequency distribution.

    Tokens appearing in > threshold fraction of non-null rows are candidates.
    Filters by column type expectations.

    Returns:
        List of {"token": str, "frequency": float, "count": int} dicts
    """
    total_rows = series.drop_nulls().len()
    if total_rows == 0:
        return []

    # Clean, split, get unique tokens per row, then count across rows
    cleaned = series.drop_nulls().cast(pl.String).str.to_lowercase().str.replace_all(r'[,.;:]', '')
    # Each row -> list of unique tokens
    per_row = cleaned.str.split(" ").map_elements(
        lambda tokens: list(set(t for t in tokens if t)), return_dtype=pl.List(pl.String)
    )
    row_counts = per_row.explode().to_frame("tok").filter(
        pl.col("tok") != ""
    ).group_by("tok").len().sort("len", descending=True)

    # Known stopword candidates by type
    known_stopwords = {
        "name": {"inc", "llc", "ltd", "corp", "co", "the", "of", "and", "group",
                 "pty", "gmbh", "sa", "ag", "limited", "incorporated", "corporation"},
        "address": {"suite", "ste", "floor", "fl", "unit", "apt", "building", "bldg",
                    "room", "rm", "dept", "level"},
    }
    known = known_stopwords.get(col_type, set())

    suggestions = []
    for row in row_counts.head(n * 3).iter_rows():
        token, count = row[0], row[1]
        freq = count / total_rows
        if freq >= threshold or token in known:
            suggestions.append({
                "token": token,
                "frequency": round(freq, 3),
                "count": count,
                "known": token in known,
            })
        if len(suggestions) >= n:
            break

    return suggestions


# ---------------------------------------------------------------------------
# Alias suggestion
# ---------------------------------------------------------------------------

def _alias_group_key(token: str) -> str:
    """Strip all non-alnum for grouping (O'Brien/OBrien -> obrien)."""
    return re.sub(r'[^a-z0-9]', '', token.lower())


def suggest_aliases(series: pl.Series, n: int = 30) -> list:
    """Find punctuation variants (O'Brien/OBrien, AT&T/ATT, Co-Op/Coop).

    Does NOT detect semantic aliases (Blvd/Boulevard) -- those need a dictionary.
    """
    # Tokenize and count via Polars
    df = series.drop_nulls().cast(pl.String).str.split(" ").explode().to_frame("tok")
    df = df.filter(pl.col("tok") != "")
    token_counts = df.group_by("tok").len()

    # Group by alias key in Python (complex regex per token)
    groups = {}
    for token, count in token_counts.iter_rows():
        key = _alias_group_key(token)
        if not key:
            continue
        if key not in groups:
            groups[key] = []
        groups[key].append((token, count))

    # Only suggest groups with multiple variants
    aliases = []
    for canonical, variants in groups.items():
        if len(variants) > 1:
            total = sum(c for _, c in variants)
            aliases.append({
                "canonical": canonical,
                "variants": [{"raw": v, "count": c} for v, c in sorted(variants, key=lambda x: -x[1])],
                "total_count": total,
            })

    # Sort by total count descending
    aliases.sort(key=lambda x: -x["total_count"])
    return aliases[:n]


# ---------------------------------------------------------------------------
# Data quality summary
# ---------------------------------------------------------------------------

def data_quality_summary(df: pl.DataFrame, columns: Optional[list] = None) -> dict:
    """Generate data quality summary for selected columns.

    Returns:
        Dict with per-column stats: null_pct, unique_pct, duplicates, etc.
    """
    if columns is None:
        columns = df.columns

    summary = {}
    total_rows = df.height

    for col in columns:
        if col not in df.columns:
            continue

        series = df[col]
        null_count = series.null_count()
        non_null = total_rows - null_count
        n_unique = series.n_unique()

        # Value length stats (for string columns)
        lengths = None
        if series.dtype == pl.String:
            len_series = series.drop_nulls().cast(pl.String).str.len_chars()
            if len_series.len() > 0:
                lengths = {
                    "min": int(len_series.min()),
                    "max": int(len_series.max()),
                    "mean": round(float(len_series.mean()), 1),
                }

        summary[col] = {
            "total_rows": total_rows,
            "null_count": null_count,
            "null_pct": round(null_count / total_rows * 100, 1) if total_rows > 0 else 0.0,
            "non_null": non_null,
            "unique_count": n_unique,
            "unique_pct": round(n_unique / total_rows * 100, 1) if total_rows > 0 else 0.0,
            "duplicate_count": total_rows - n_unique,
            "lengths": lengths,
        }

    return summary


# ---------------------------------------------------------------------------
# Full analysis pipeline
# ---------------------------------------------------------------------------

def analyze_column(series: pl.Series, col_name: str,
                   col_type: Optional[str] = None,
                   unicode_mode: str = "profile_only",
                   top_n: int = 30) -> dict:
    """Run full signal analysis on a single column.

    Args:
        series: Polars Series
        col_name: Column name (for labeling)
        col_type: Override column type, or None to auto-detect
        unicode_mode: 'profile_only' or 'skip'
        top_n: Number of top tokens/suggestions to return

    Returns:
        Dict with all analysis results for the column
    """
    if col_type is None:
        col_type = detect_column_type(series)

    result = {
        "column": col_name,
        "detected_type": col_type,
        "top_tokens_raw": top_tokens(series, tier="raw", n=top_n),
        "top_tokens_clean": top_tokens(series, tier="clean", n=top_n),
        "suggested_stopwords": suggest_stopwords(series, col_type=col_type),
        "suggested_aliases": suggest_aliases(series),
    }

    if unicode_mode == "profile_only":
        result["unicode_profile"] = unicode_profile_column(series)

    return result


def analyze_dataset(df: pl.DataFrame, columns: list,
                    col_types: Optional[dict] = None,
                    unicode_mode: str = "profile_only",
                    output_dir: Optional[str] = None) -> dict:
    """Run signal analysis on multiple columns of a dataset.

    Args:
        df: Polars DataFrame
        columns: List of column names to analyze
        col_types: Optional dict of {col_name: type_override}
        unicode_mode: 'profile_only' or 'skip'
        output_dir: If set, write stopwords.json and aliases.json

    Returns:
        Dict with per-column analysis + data quality summary
    """
    col_types = col_types or {}

    results = {
        "data_quality": data_quality_summary(df, columns),
        "columns": {},
        "_raw_series": {},
    }

    all_stopwords = {}  # Keyed by column type: {"name": set(), "address": set(), ...}
    all_aliases = {}

    for col in columns:
        if col not in df.columns:
            continue

        col_type = col_types.get(col)
        analysis = analyze_column(df[col], col, col_type=col_type,
                                  unicode_mode=unicode_mode)
        results["columns"][col] = analysis
        results["_raw_series"][col] = df[col].to_list()

        # Aggregate stopwords by type (known always included, others need 0.2+ frequency)
        col_type = analysis["detected_type"]
        if col_type not in all_stopwords:
            all_stopwords[col_type] = set()
        for sw in analysis["suggested_stopwords"]:
            if sw["known"] or sw["frequency"] >= 0.2:
                all_stopwords[col_type].add(sw["token"])

        # Aggregate aliases ({"variant_clean": "canonical_stripped"} for normalized())
        for alias in analysis["suggested_aliases"]:
            if len(alias["variants"]) > 1:
                canonical = alias["canonical"]
                for variant in alias["variants"]:
                    variant_clean = clean(variant["raw"])
                    if variant_clean != canonical:
                        all_aliases[variant_clean] = canonical

    results["aggregated_stopwords"] = {k: sorted(v) for k, v in all_stopwords.items()}
    results["aggregated_aliases"] = all_aliases

    # Write config files if output_dir specified
    if output_dir:
        out = Path(output_dir)

        with open(out / "stopwords.json", "w") as f:
            json.dump({k: sorted(v) for k, v in all_stopwords.items()}, f, indent=2)

        with open(out / "aliases.json", "w") as f:
            json.dump(all_aliases, f, indent=2, ensure_ascii=False)

    return results
