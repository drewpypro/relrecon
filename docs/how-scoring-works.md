# How Scoring Works

The matching pipeline produces two scores per matched record: a **name score** (how the pair was found) and an **address score** (confidence signal or filter).
These are independent systems that run sequentially.

## Systems Overview

| System | Module | Config | Job |
|---|---|---|---|
| **Name matching** | matching.py | Recipe `match_fields` | Finds candidate pairs by comparing names |
| **Address scoring** | address.py | Recipe `address_support` + config files | Scores address similarity on matched pairs |

Address scoring involves three subsystems. Each does one job:

| System | Module | Config | Job |
|---|---|---|---|
| **Normalization** | normalize.py | `aliases.json`, `stopwords.json` | Text cleanup: lowercase, remove commas/periods/semicolons/colons (hyphens/apostrophes/ampersands preserved), expand aliases (blvd→boulevard), remove stopwords |
| **Parsing** | address.py | `address_patterns.json` (or libpostal) | Structural decomposition: extract street name, suffix, unit, state, zip |
| **Scoring** | address.py + RapidFuzz | -- | String similarity scoring on full address + street name, weighted combination |

These are stacked, not alternatives. Every address pair goes through all three.

## Name Scoring

Name matching finds candidate pairs. Two methods:

**Exact** (`method: exact`): Polars inner join on normalized name values.
If the names are identical after tier normalization, they match.
Score is always **100**.

**Fuzzy** (`method: fuzzy`): RapidFuzz `cdist` computes a full score matrix (C++ backend, no Python loops).
Each source record gets the best-scoring destination above the threshold.
Score is **0-100** (e.g. 85.7 means 85.7% similarity).

Both methods try tiers in recipe order (e.g. `tiers: [raw, clean]`).
If a record matches on multiple tiers, the earlier tier in the list wins.

| Setting | Where | Default |
|---|---|---|
| Method | `match_fields.method` | `exact` |
| Tiers | `match_fields.tiers` | `[raw, clean]` |
| Threshold (fuzzy only) | `match_fields.threshold` | `80` |
| Scorer (fuzzy only) | `match_fields.scorer` | `token_sort_ratio` |

Available scorers: `token_sort_ratio`, `token_set_ratio`, `ratio`, `partial_ratio`, `WRatio`.

Name matching runs first. Address scoring runs only on records that passed name matching.

## Address Scoring

### Which Tools Use What

| Tool | normalize.py (clean/normalized) | address.py (libpostal/tokenizer) | RapidFuzz |
|---|---|---|---|
| Signal analysis | Yes -- clean tier for token analysis | No | No |
| Name matching | Yes -- tier depends on recipe `tiers` list | No | Only if `method: fuzzy` in recipe |
| Address scoring | Yes -- all three tiers (hardcoded) | Yes -- street name extraction | Yes -- always (full + street score) |

Note: "fuzzy" in name matching (RapidFuzz cdist on names) and "fuzzy" in address scoring (RapidFuzz token_sort_ratio on addresses) are unrelated. Name matching method (exact/fuzzy) does not affect address scoring -- address scoring runs the same way on every matched pair regardless of how the name match was found.

### Execution Order

For each address pair, per normalization tier:

```
1. NORMALIZE  apply_tier(address, tier, aliases, stopwords)
              raw: as-is | clean: lowercase + remove ,.;: | normalized: clean + aliases + stopwords
                    │
2. SCORE      rfuzz.token_sort_ratio(src_normalized, dst_normalized)
              produces full_score (0-100)
                    │
3. PARSE      parse_address(normalized_text, parser_mode)
              extracts street_name using libpostal or built-in tokenizer
                    │
4. STREET     rfuzz.ratio(street_src, street_dst)
              produces street_score (0-100), street_match = (street_score >= 80)
                    │
5. WEIGHT     street_match? weighted = street*0.6 + full*0.4
              no street?    weighted = full_score
```

This runs for each tier (raw, clean, normalized) and each comparison (merged<>merged, addr1<>addr1, addr1<>addr2, addr2<>addr1, addr2<>addr2).
The best weighted score across all tiers and comparisons wins.

**Why normalize before parse:** The parser sees cleaner input.
Alias expansion (blvd→boulevard) helps the built-in tokenizer match street suffixes.
Stopword removal reduces noise.
libpostal handles raw input fine, but clean input doesn't hurt it.

### Worked Example

Source: `"123 Main Blvd Suite 200"` vs Dest: `"123 MAIN BOULEVARD STE 200"`

Aliases: `{"blvd": "boulevard", "ste": "suite"}`  
Stopwords: `{"address": ["suite"]}`

#### RAW tier

```
normalize:  "123 Main Blvd Suite 200"  vs  "123 MAIN BOULEVARD STE 200"
full_score: token_sort_ratio → ~72 (case differs, blvd != boulevard)
parse:      street_name: "123 main"    vs  street_name: "123 main"
street:     ratio → 100, street_match = true
weighted:   100 * 0.6 + 72 * 0.4 = 88.8
```

#### CLEAN tier

```
normalize:  "123 main blvd suite 200"  vs  "123 main boulevard ste 200"
full_score: token_sort_ratio → ~82 (blvd != boulevard, suite != ste)
parse:      street_name: "123 main"    vs  street_name: "123 main"
street:     ratio → 100, street_match = true
weighted:   100 * 0.6 + 82 * 0.4 = 92.8
```

#### NORMALIZED tier (with aliases + stopwords)

```
normalize:  "123 main boulevard 200"   vs  "123 main boulevard 200"
            (blvd→boulevard, ste→suite, then "suite" removed as stopword)
full_score: token_sort_ratio → 100 (identical)
parse:      street_name: "123 main"    vs  street_name: "123 main"
street:     ratio → 100, street_match = true
weighted:   100 * 0.6 + 100 * 0.4 = 100.0
```

**Result:** normalized tier wins with score 100.0, comparison addr1<>addr1, street_match true.

### What's Configurable vs Hardcoded

| Setting | Where | Configurable? |
|---|---|---|
| Address fields (source/dest) | Recipe `address_support.source/destination` | Yes |
| Parser mode (auto/libpostal/default) | Recipe `address_support.parser` | Yes |
| Score threshold | Recipe `address_support.threshold` | Yes |
| Tiers tried | Recipe `address_support.tiers` | Yes -- default `[raw, clean, normalized]` |
| Street weight (60/40) | Hardcoded in `score_address_pair` | No |
| Street match threshold (>=80) | Hardcoded in `score_address_pair` | No |
| Comparisons tried | Hardcoded in `score_address_pair` | No -- always all 5 combinations |

## Understanding the Report Columns

The report shows several tier-related columns that can be confusing because they come from **independent systems**:

| Column | What it tells you | Set by |
|---|---|---|
| `match_tier` | Which normalization made the names join (how the pair was found) | Name matching |
| `addr_score` | Best similarity score across all address tiers | Address scoring |
| `addr_tier` | Which tier produced that best score (informational only) | Address scoring |
| `addr_comparison` | Which field combo scored best -- addr1<>addr1, merged<>merged, etc. (informational only) | Address scoring |

`match_tier` and `addr_tier` are independent and often different.
Example:

```
Source name: "ACME CORP"     Dest name: "Acme Corp"
Source addr: "123 Main St"   Dest addr: "123 Main St"

Name matching: raw fails (case differs), clean matches → match_tier: clean
Address scoring: raw scores 100 (identical strings) → addr_tier: raw

Report shows: match_tier=clean, addr_tier=raw
```

This is correct -- the name needed cleaning to match, but the addresses were already identical raw.

Note: the report shows **original values** (pre-normalization) alongside tier metadata.
The tier tells you what normalization was applied internally to find the match or produce the score.

## Address Threshold and Cascading

When `address_support.threshold` is set (e.g. 60), it acts as a **cascade filter**, not a record deletion:

1. Record matches on name in Step 1 → address score = 45 → below threshold

2. Record is removed from Step 1 results but **cascades to Step 2**

3. Step 2 tries matching with a different destination population

4. If Step 2 produces addr_score >= 60 → record is kept

5. If no step produces a passing score → record appears in the Analysis tab with `reason_code: addr_below_threshold` and `best_rejected_score: 45`

No records are lost.
The threshold just says "this match isn't good enough, try the next step."

## Name Tiers vs Address Tiers

Name and address tiers are independent systems with independent config (ADR-002, Option B):

- **Name tiers**: `match_fields.tiers` -- controls which tiers are tried for name matching.
  Position in list = priority (first wins ties).

- **Address tiers**: `address_support.tiers` -- controls which tiers are tried for address scoring.
  Default: `[raw, clean, normalized]` when omitted.
  Position in list determines tie-breaking (first tier wins at equal scores).

## Reserved Column Names

The join engine uses the `_dst` suffix to disambiguate destination columns when source and destination share column names (e.g. both populations having `hq_addr1`).
Source data column names should not end in `_dst` to avoid conflicts with this internal naming.
