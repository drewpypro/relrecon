# Benchmark Test Cases

## Overview

Each option (A/B/C) runs the **same matching task** on the **same generated dataset** and produces results to `benchmarks/results/`. We measure wall time, peak memory, and match quality.

---

## Dataset Generation (`generate_data.py`)

| Parameter | Value | Rationale |
|---|---|---|
| Source rows (Pop1-like) | 15,000 | Matches real work dataset size |
| Target rows (core_parent-like) | 500 | Realistic trusted reference size |
| Match rate | ~70% of source have a target match | Simulates real-world partial overlap |
| Noise types | Case, punctuation, spacing, abbreviations | Simulates real data quality issues |
| Seed | Fixed (42) | Reproducible across runs |
| Columns | l3_fmly_nm, vendor_id, hq_addr1, hq_addr2 | Key matching fields only |
| Core throttle | 8 cores | Emulates HP ZBook |

### Noise Distribution (applied to matching source records)

| Noise Type | Probability | Example |
|---|---|---|
| Lowercase | 20% | "Nexacore Inc" → "nexacore inc" |
| Uppercase | 15% | "Nexacore Inc" → "NEXACORE INC" |
| Trailing comma | 15% | "Nexacore Inc" → "Nexacore Inc," |
| Trailing period | 10% | "Nexacore Inc" → "Nexacore Inc." |
| Extra whitespace | 10% | "Nexacore Inc" → "  Nexacore Inc  " |
| No noise | 30% | "Nexacore Inc" → "Nexacore Inc" |

### Address Noise Distribution

| Noise Type | Probability | Example |
|---|---|---|
| Lowercase | 20% | "500 Technology Drive" → "500 technology drive" |
| Abbreviation swap | 20% | "500 Technology Street" → "500 Technology St" |
| Expansion swap | 10% | "500 N Main" → "500 North Main" |
| No noise | 50% | "500 Technology Drive" → "500 Technology Drive" |

---

## Test Cases

### TC-01: Name Matching — Fuzzy (score_cutoff=85%)

> **Note:** The production matching engine uses exact matching (Raw/Clean) for names, not fuzzy. This test case uses fuzzy matching specifically to stress-test library speed on the most computationally expensive operation. It measures relative library performance, not the actual matching logic.

**What:** Each option matches source `l3_fmly_nm` against target `vendor_name` using fuzzy string comparison at 85% threshold.

**Input:** 15k source rows, 500 target rows (generated dataset).

**Measurements:**
- Wall time (seconds)
- Peak memory (MB)
- Total matches found
- Match overlap between options (are they finding the same matches?)

**Expected behavior:**
- All options should find roughly similar match counts (within ~10% of each other)
- Option C should be significantly faster than A and B
- Option A should be the slowest

**Option-specific implementation:**
| Option | Library | Matching Function |
|---|---|---|
| A | difflib | `SequenceMatcher.ratio()` — loops with iterrows |
| B | fuzzywuzzy | `fuzz.ratio()` — loops with iterrows |
| C | rapidfuzz | `process.extractOne()` with `fuzz.ratio` scorer |

**Note:** Options A and B are capped at 1,000 rows with projected extrapolation to 15k. Option C runs full 15k.

---

### TC-02: Name Matching — Clean Preprocessing + Exact

**What:** Apply Clean normalization (lowercase, strip spaces/punctuation) then exact match. Tests the data wrangling speed difference between Pandas and Polars.

**Input:** Same generated dataset.

**Measurements:**
- Wall time for normalization step only
- Wall time for matching step only
- Total exact matches found (should be identical across options)

**Option-specific implementation:**
| Option | Normalization | Match |
|---|---|---|
| A | Python str methods in loop | dict lookup (one match per source row) |
| B | Pandas `.str.lower().str.strip()` + dedup | dict lookup (one match per source row) |
| C | Polars `.str.to_lowercase().str.strip_chars()` | Polars join (deduplicated) |

> **Note:** All options must produce semantically equivalent results (one best match per source row). Pandas merge can produce cartesian products on duplicate keys — results are deduplicated before counting.

---

### TC-03: Address Matching — Token Overlap with Street Name Weighting

**What:** Merge addr1+addr2, tokenize, compute weighted token overlap (street name boosted). Tests the full address matching pipeline at each normalization tier.

**Input:** Same generated dataset, address fields only.

**Measurements:**
- Wall time (seconds)
- Peak memory (MB)
- Distribution of match scores (histogram buckets: 0-59%, 60-79%, 80-99%, 100%)

**Option-specific implementation:**
| Option | Tokenizer | Scorer |
|---|---|---|
| A | Python `split()` | Manual set intersection / `SequenceMatcher` |
| B | Pandas + fuzzywuzzy `token_sort_ratio` | fuzzywuzzy |
| C | Polars + rapidfuzz `token_sort_ratio` | rapidfuzz |

---

### TC-04: Combined Pipeline — Name + Address + Date Gate

**What:** Full matching pipeline: filter by date (2-year rule), match names (Clean exact), score addresses (fuzzy), produce final match with confidence score. This is the closest to what the real matching engine will do.

**Input:** Same generated dataset with date fields added (70% within 2 years, 30% stale).

**Measurements:**
- Wall time end-to-end (seconds)
- Peak memory (MB)
- Matches found (after date gate)
- Matches filtered by date rule

**Option-specific implementation:**
| Option | Date Filter | Name Match | Address Score |
|---|---|---|---|
| A | Python loop | difflib in loop | difflib in loop |
| B | Pandas boolean mask | fuzzywuzzy apply | fuzzywuzzy apply |
| C | Polars filter expr | rapidfuzz extractOne | rapidfuzz token_sort_ratio |

---

## Output Format

Each benchmark writes to `benchmarks/results/`:

### `summary.json`
```json
{
  "generated_at": "2026-04-11T14:00:00",
  "hardware": {"cores_used": 8, "note": "throttled to emulate ZBook"},
  "dataset": {"source_rows": 15000, "target_rows": 500, "seed": 42},
  "results": {
    "A": {
      "TC-01": {"time_s": 0, "projected_15k_s": 0, "memory_mb": 0, "matches": 0, "rows_tested": 1000},
      ...
    },
    "B": { ... },
    "C": { ... }
  }
}
```

### `summary_snapshot.md`
Markdown-formatted report with tables, speedup ratios, and match counts per test case. Renders in Gitea and local editors.

---

## Success Criteria

The benchmark validates all options and selects the fastest that meets the requirements:

1. Winning option completes all test cases on full 15k rows in under 60 seconds total
2. Compare relative speedup across all options on fuzzy matching (TC-01)
3. Match quality (counts + overlap) is comparable across all options (within 10%)
4. Peak memory stays under 1GB for all options
5. Winning option is selected based on best overall performance across all test cases
