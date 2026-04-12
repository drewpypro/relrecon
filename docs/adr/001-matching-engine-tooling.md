# ADR-001: Matching Engine Tooling Selection

**Status:** Accepted  
**Date:** 2026-04-11  
**Author:** Bogoy (AI Assistant)  
**Deciders:** Drew (bobturdhands)

---

## Context

We need to build a data matching engine that:
- Reconciles ~15,000 rows × 232 columns from third-party datasets
- Performs multi-tier normalization (Raw, Clean, Normalized) on name and address fields
- Matches hierarchical L1/L3 relationships across populations
- Produces an auditable Excel report with confidence scores and rule explanations
- Runs locally on developer hardware before scaling to larger infrastructure

### Hardware Profiles

| Machine | CPU | Cores | RAM | Use |
|---|---|---|---|---|
| HP ZBook (work laptop) | AMD Ryzen 9 PRO 7940HS | 8 cores / 16 threads | 64 GB | Primary development & report generation |
| Minisforum X1 AI PC (home) | Intel (16 threads) | 16 threads | 60 GB | Testing & automation |

### Key Bottleneck

Address matching is the critical performance bottleneck. With naive pairwise comparison:
- Pop1 (15k) × core_parent (N) = potentially millions of fuzzy string comparisons
- Each comparison involves tokenization, normalization, and scoring
- Current approach takes "forever" on the ZBook

---

## Options

### Option A: Python + Standard Library (difflib, iterrows)

**Approach:** Pure Python with `csv` or `pandas` for loading, `difflib.SequenceMatcher` for fuzzy matching, `iterrows()` for row-by-row processing.

```python
import pandas as pd
from difflib import SequenceMatcher

for idx, pop1_row in pop1_df.iterrows():
    for _, core_row in core_df.iterrows():
        ratio = SequenceMatcher(None, pop1_row['l3_fmly_nm'], core_row['Vendor Name']).ratio()
```

**Pros:**
- Zero dependencies beyond Python stdlib
- Simple to understand and debug
- No installation issues behind corporate proxies/firewalls

**Cons:**
- `iterrows()` is the slowest way to iterate a DataFrame (~100x slower than vectorized ops)
- `difflib.SequenceMatcher` is pure Python — no C optimization
- No parallelism — single-threaded, wastes 15/16 cores
- At 15k × N with fuzzy matching: estimated **30-60+ minutes** per run
- Completely unacceptable for iterative development (change logic → wait an hour)

**Verdict:** ❌ **Not recommended.** Included as a baseline to illustrate why optimization matters.

---

### Option B: Python + Pandas + fuzzywuzzy

**Approach:** Pandas for data manipulation, fuzzywuzzy for fuzzy matching, manual normalization with `.str` accessors. Vectorized where possible, but fuzzy matching still requires apply/loops.

```python
import pandas as pd
from fuzzywuzzy import fuzz

def match_names(pop1_name, core_names):
    return max(core_names, key=lambda x: fuzz.ratio(pop1_name, x))

pop1_df['best_match'] = pop1_df['l3_fmly_nm'].apply(lambda x: match_names(x, core_names))
```

**Pros:**
- Pandas is well-known; large community and documentation
- fuzzywuzzy provides multiple scoring algorithms (ratio, partial_ratio, token_sort_ratio, token_set_ratio)
- `.str` accessors handle bulk normalization (lowercase, strip, replace)
- Adequate for small-to-medium datasets

**Cons:**
- fuzzywuzzy uses `difflib` under the hood (pure Python) unless `python-Levenshtein` is installed
- Even with `python-Levenshtein`, still significantly slower than RapidFuzz
- Pandas is single-threaded for most operations; GIL limits parallelism
- Memory overhead: Pandas copies DataFrames frequently (15k × 232 cols ≈ significant RAM)
- Address matching at scale still slow: estimated **5-15 minutes** per run
- `fuzzywuzzy` is effectively unmaintained (last release 2021)

**Verdict:** ⚠️ **Acceptable for prototyping**, but will hit performance walls at production dataset size. Not recommended for the final solution.

---

### Option C: Python + Polars + RapidFuzz + libpostal (Recommended)

**Approach:** Polars for all data loading/filtering/joins (multi-threaded, lazy evaluation), RapidFuzz for fuzzy string matching (C++ backend), libpostal for international address parsing (optional, with built-in fallback tokenizer), blocking strategy to reduce comparison space.

```python
import polars as pl
from rapidfuzz import fuzz, process

# Polars: lazy evaluation, multi-threaded filtering
pop1 = lf.filter(pl.col("vendor_id").str.starts_with("V7")).collect()

# Blocking: only compare within same state/city
blocks = pop1.group_by("parsed_state")

# RapidFuzz: C++ backend, 10-100x faster than fuzzywuzzy
matches = process.extract(query, choices, scorer=fuzz.token_sort_ratio, score_cutoff=60)
```

**Pros:**
- **Polars:** 5-10x faster than Pandas on filtering/joins, native multi-threading (uses all 8/16 cores), lazy evaluation optimizes query plans, lower memory footprint (Arrow-backed)
- **RapidFuzz:** C++ compiled backend, 10-100x faster than fuzzywuzzy, same API surface, actively maintained, supports `process.extract` for batch matching
- **libpostal** (optional)**:** International address parser (200+ countries), splits addresses into structured components, handles abbreviations and normalization. Built-in two-pass tokenizer as zero-dependency fallback
- **Blocking strategy:** Parse addresses → group by state/city/zip → only compare within blocks. Reduces comparisons from 15k × N to small block sizes (e.g., 50 × 5)
- **Address token scoring:** Compare parsed components independently, weight street name highest
- Estimated runtime: **seconds to low minutes** on the ZBook

**Cons:**
- More dependencies to install (polars, rapidfuzz, openpyxl; libpostal optional)
- Polars API differs from Pandas (learning curve, but well-documented)
- libpostal requires C library install + ~2GB model data (one-time setup)
- Blocking requires choosing good block keys (bad keys = missed matches)

**Verdict:** ✅ **Recommended.** Best balance of performance, accuracy, and developer ergonomics for this use case.

### Address Parsing: libpostal vs usaddress

The dataset contains **global addresses** (not just US), which rules out US-only parsers.

| | usaddress | libpostal (pypostal) |
|---|---|---|
| Coverage | US only | 200+ countries, every language |
| Training data | ~1M US addresses | OpenStreetMap (billions of addresses) |
| Approach | CRF model | Statistical NLP on OSM data |
| Normalization | No (parsing only) | Yes — built-in address normalization |
| Speed | Fast | Fast (C library with Python bindings) |
| Install | `pip install usaddress` | C library install + ~2GB model download, then `pip install postal` |

libpostal handles parsing AND normalization in one step (e.g., "6th Ave" → "6th Avenue", "N" → "North", "Ste" → "Suite") and works with international address formats. The heavier install (~2GB models) is a one-time cost and trivial on 64GB machines.

**Decision:** Use **libpostal** (via `pypostal` Python bindings) instead of usaddress.

*Credit: [@realoksi](https://git.drewpy.pro/realoksi) suggested libpostal for its international coverage and Python bindings.*

### Dependencies

```
polars          — data loading, filtering, joins, population splits, lazy evaluation
rapidfuzz       — fuzzy string matching (names + addresses), C++ backend
libpostal       — C library for international address parsing + normalization
pypostal        — Python bindings for libpostal
openpyxl        — Excel output with formatting
```

---

### Option D: Rust (dedupe/custom) or PySpark

**Approach:** Either rewrite the matching engine in Rust for maximum performance, or use PySpark/Dask for distributed computing.

**Rust:**
- Maximum single-machine performance
- No GIL, true parallelism, zero-copy memory
- But: loses the entire data science ecosystem (no Polars-equivalent ease, no RapidFuzz, no libpostal)
- Development time: 5-10x longer for equivalent functionality
- Overkill for 15k rows — this isn't a Big Data problem, it's an algorithmic one

**PySpark/Dask:**
- Designed for datasets that don't fit in memory (millions+ rows)
- Cluster overhead is wasteful for 15k rows on a 64GB machine
- Complex setup, harder to debug locally
- Would make sense if dataset grows to 500k+ rows

**Verdict:** ⚠️ **Over-engineered for current scale.** Rust makes sense if this becomes a production service processing millions of records daily. PySpark/Dask if the dataset grows 50-100x. Neither is justified for 15k × 232 on a 64GB machine.

---

## Decision

**Accepted.** Option C selected: Polars + RapidFuzz + libpostal (optional) + openpyxl.

## Rationale

The performance bottleneck is not a language problem — it's an algorithmic one. The combination of:

1. **Blocking** (reduce comparison space by 90%+)
2. **RapidFuzz** (C++ fuzzy matching, 10-100x faster)
3. **Polars** (multi-threaded data ops, uses all cores)
4. **libpostal** (structured address parsing for component-level matching, optional with built-in fallback)

...solves the "takes forever" problem without leaving the Python ecosystem. The ZBook's 8-core Ryzen 9 + 64GB RAM is more than adequate — we just need to stop wasting those resources on single-threaded pure-Python loops.

## Pipeline Stages

```
1. Signal Analysis    → Profile columns, generate stopwords/aliases
2. Configure Recipe   → Define sources, field mappings, matching steps
3. Run Matching       → Execute recipe (normalization, matching, scoring)
4. Generate Report    → Excel output with matched + analysis tabs
```

Signal analysis is a prerequisite that bootstraps the normalization config. It should be runnable independently against any dataset/column combination.

## Migration Path

```
Current state (Option A/B) → Option C (immediate)
                            → Option D (only if dataset grows 50-100x or becomes a service)
```

## Consequences

- Team needs to learn Polars API (different from Pandas, but cleaner)
- Address matching quality improves (libpostal parses international addresses; built-in tokenizer handles common patterns)
- Report generation drops from 30-60 min to under 2 minutes
- Matching logic becomes testable with fast iteration cycles
