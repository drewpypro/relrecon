# Signal Analysis

Signal analysis is a **pre-pipeline profiling tool**.
It examines your source data to help you configure normalization before running the matching pipeline.

## Where It Fits

```
Signal Analysis (you are here)
  │
  │  Produces: suggested stopwords, aliases, data quality stats
  │  Outputs:  stopwords.json, aliases.json (optional)
  │
  ▼
Matching Pipeline
  │
  │  Consumes: stopwords.json, aliases.json (via recipe normalization config)
  │  Uses them in: name matching (normalized tier), address scoring (normalized tier)
  │
  ▼
Report Output
```

Signal analysis does NOT run during the pipeline.
It's a separate CLI command you run once (or periodically) to understand your data and tune your config.

## Quick Start

```bash
# Analyze all string columns
python -m src --analyze data/your_file.csv

# Auto-detect name/address columns only
python -m src --analyze data/your_file.csv --columns auto

# Analyze specific columns
python -m src --analyze data/your_file.csv --columns l3_fmly_nm,hq_addr1

# Unicode profile only, top 10 items
python -m src --analyze data/your_file.csv --sections unicode --top 10

# Save suggested config files
python -m src --analyze data/your_file.csv --save-config config/suggested/
```

## CLI Options

| Flag | What it does | Default |
|---|---|---|
| `--analyze FILE` | Run signal analysis instead of matching pipeline | -- |
| `--columns` | `auto` to detect name/address, or comma-separated names | All string columns |
| `--sections` | Filter report: quality, tokens, stopwords, aliases, unicode, suggestions | All |
| `--top N` | Max items per section (0 = show all) | 15 |
| `--save-config DIR` | Write suggested `stopwords.json` and `aliases.json` to DIR (must exist) | Don't write |

## What the Report Shows

### Data Quality

Per-column stats: row count, null %, unique %, duplicate count.
Use this to spot columns with high nulls or low uniqueness (possible IDs, not names).

### Top Tokens (Raw and Clean)

Most frequent tokens at raw tier (as-is) and clean tier (lowercased, commas/periods/semicolons/colons removed -- hyphens/apostrophes/ampersands preserved).
Compare them to spot case/punctuation variants that clean normalization already handles.

### Suggested Stopwords

Tokens that appear in a high percentage of rows.
Flagged as "known" if they match common patterns (Inc, LLC, Suite, Floor, etc.).

**How to use:** Review the suggestions.
Copy the ones that make sense into your `config/stopwords.json`.
Not all high-frequency tokens are stopwords -- "Street" might be frequent but is meaningful for address matching.

### Alias Groups

Tokens that differ in case, punctuation, hyphens, apostrophes, or ampersands.
Groups variants like O'Brien/OBrien, AT&T/ATT, Co-Op/Coop, Inc/Inc./INC.

Variants already handled by `clean()` (case + commas/periods/semicolons/colons)
are shown in the report but excluded from `aliases.json` since they're redundant.
Only variants that survive `clean()` (hyphens, apostrophes, slashes, ampersands)
are written to the config.

**What this does NOT detect:** Semantic aliases like Blvd/Boulevard or St/Street.
Those need a pre-built dictionary (your `config/aliases.json`) or libpostal.

### Character Ranges

Distribution of character types per column (ascii_alnum, ascii_punct_space, cyrillic, cjk, latin, etc.).
Always shown. Tells you immediately what scripts exist in your data.

When non-ASCII characters are detected, shows sample rows with the actual values
and which ranges were found. Unknown character and mixed script warnings shown when applicable.

### Aggregated Suggestions

Combined stopwords and aliases across all analyzed columns, grouped by detected column type (name vs address).
This is what gets written to files when you use `--save-config`.

## Workflow

1. **Run analysis** on your source data:

   ```bash
   python -m src --analyze data/tp_multi_pop_dataset.csv
   ```

2. **Review the output.** Look for:
   - Stopword candidates that make sense for your use case
   - Alias groups that indicate messy data
   - Unicode flags that might need attention
   - Data quality issues (high nulls, low uniqueness)

3. **Save suggestions** (optional):

   ```bash
   python -m src --analyze data/your_file.csv --save-config config/
   ```

4. **Curate the config files.** The saved files are suggestions, not gospel.
   Remove stopwords that are meaningful for your matching.
   Add semantic aliases (blvd/boulevard) that signal analysis can't detect.

5. **Reference in your recipe:**

   ```yaml
   normalization:
     stopwords: config/stopwords.json
     aliases: config/aliases.json
   ```

6. **Run the pipeline.** The matching engine uses your curated config for the normalized tier.

## Gotchas

- **`--save-config config/` overwrites existing files.**
  If you've already curated your stopwords/aliases, point to a different directory first (e.g. `--save-config config/suggested/`) and diff before replacing.

- **`--columns auto` picks name and address columns by heuristic.**
  It looks for corporate suffixes (Inc, LLC) and address tokens (Street, Ave).
  Without `--columns`, all string columns are analyzed.
  If auto-detect misses columns, specify them explicitly with `--columns col1,col2`.

- **Stopword suggestions are frequency-based, not semantic.**
  A token appearing in 20%+ of rows gets flagged.
  "Services" might be flagged but removing it could hurt matching.
  Always review before adopting.

- **Signal analysis does not use libpostal.**
  It profiles raw text with basic tokenization.
  See Issue #88 for future libpostal-powered address profiling.

- **Aliases detect punctuation variants beyond what clean() handles.**
  O'Brien/OBrien, AT&T/ATT, Co-Op/Coop will be grouped and saved.
  Case-only variants (Inc/INC/inc.) are shown but not saved (clean() handles them).
  Semantic aliases like Blvd/Boulevard will NOT be detected -- add those manually.
