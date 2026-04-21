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

# Excel output (one sheet per section)
python -m src --analyze data/your_file.csv --signal-format xlsx --signal-output output/signal_report.xlsx

# Both markdown and Excel
python -m src --analyze data/your_file.csv --signal-format both --signal-output output/signal_report

# Excel with all items (no top-N limit)
python -m src --analyze data/your_file.csv --signal-format xlsx --top 0
```

## CLI Options

| Flag | What it does | Default |
|---|---|---|
| `--analyze FILE` | Run signal analysis instead of matching pipeline | -- |
| `--columns` | `auto` to detect name/address, or comma-separated names | All string columns |
| `--sections` | Filter report: quality, tokens, stopwords, aliases, unicode, suggestions, singletons, duplicates, positions, lengths, numeric | All |
| `--top N` | Max items per section (0 = show all). Controls CLI display only -- does NOT affect `--save-config` output | 15 |
| `--save-config DIR` | Write suggested `stopwords.json` and `aliases.json` to DIR (must exist) | Don't write |
| `--signal-format` | Output format: `md` (markdown), `xlsx` (Excel), `both` | `md` |
| `--signal-output FILE` | Output path for report file. Auto-generates timestamp-based name if not set | Auto |


> **Note:** `--top` and `--save-config` are independent. `--top 5` caps the CLI display to 5 items per section but `--save-config` writes all suggestions that pass the frequency threshold (>= 0.2 or known patterns). The saved files are never affected by `--top`.

> **Note:** `--signal-format xlsx` produces a multi-sheet Excel workbook matching the formatting conventions of the main pipeline report. `--signal-format both` writes both `.xlsx` and `.md` files.

## What the Report Shows

### Data Quality

Per-column stats: row count, null %, unique %, duplicate count.
Use this to spot columns with high nulls or low uniqueness (possible IDs, not names).

### Top Tokens (Raw and Clean)

Most frequent tokens at raw tier (as-is) and clean tier (lowercased, commas/periods/semicolons/colons removed -- hyphens/apostrophes/ampersands preserved).
Compare them to spot case/punctuation variants that clean normalization already handles.

In addition to single tokens (topk), the analysis also produces **bigrams** (two-word pairs) and **trigrams** (three-word sequences). These help identify multi-word patterns like company names ("Financial Services") or address fragments ("PO Box") that appear frequently across records.

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

### Singleton Tokens

Tokens that appear exactly once in a column. High-value for finding:
- **Data entry typos**: "Sevices" instead of "Services"
- **Encoding artifacts**: garbled text from bad imports
- **Unique identifiers**: legitimate one-off values

Review singletons alongside near-duplicates to confirm whether they're typos or legitimate unique values.

### Near-Duplicate Tokens

Token pairs with high edit similarity (85%+ by default) detected via RapidFuzz.
Finds probable typos like "Holdngs" vs "Holdings" or "Sevices" vs "Services".

Only compares tokens of similar length (within 2 characters) for performance.
Limited to the top 200 most frequent tokens per column to avoid combinatorial explosion on large datasets.

### Token Position Frequency

Where tokens appear: first word, last word, or middle position.
Useful for entity names where suffixes like Inc/LLC should always be last.
If "Inc" appears as a first token, that record likely has a data quality issue.

### Token Length Distribution

Character count statistics per token: min, max, mean, median plus histogram.
Short tokens (1-2 chars) are usually abbreviations or noise.
Long tokens (20+) might be concatenation errors or unparsed data.

### Numeric Token Ratio

Fraction of tokens that are purely numeric vs alphabetic vs mixed.
A "name" column with 30% numeric tokens likely has data quality issues.
Helps validate column type detection.

### Character Ranges

Distribution of character types per column (ascii_alnum, ascii_punct_space, cyrillic, cjk, latin, etc.).
Always shown. Tells you immediately what scripts exist in your data.

When non-ASCII characters are detected, shows sample rows with the actual values
and which ranges were found. Unknown character and mixed script warnings shown when applicable.

### Aggregated Suggestions

Combined stopwords and aliases across all analyzed columns, grouped by detected column type (name vs address).
This is what gets written to files when you use `--save-config`.

## Excel Output

When using `--signal-format xlsx` or `--signal-format both`, the Excel workbook contains four sheets:

| Sheet | Contents |
|---|---|
| **Summary** | Top tokens preview (top 25 per tier/column) + data quality stats (including numeric %) side-by-side |
| **TopTokens** | Full detail: columnName, signalType (topk/bigram/trigram/singleton), dataTier, token, rows, freq% |
| **Alias** | Detected punctuation/case variant groups with counts |
| **NearDuplicates** | Token pairs with high edit similarity -- probable typos, with similarity % and counts |
| **TokenProfile** | Token position frequency (first/last/middle), length distribution (stats + histogram), numeric ratio per column |
| **Unicode** | Character range profiles per column (ascii, latin, cjk, etc.) |

The Summary sheet gives a quick overview. The TopTokens sheet has every token/bigram/trigram/singleton found, making it easy to sort and filter in Excel. The NearDuplicates sheet highlights probable typos. The TokenProfile sheet consolidates all token profiling data.

Formatting matches the main pipeline report conventions (blue headers, borders, conditional coloring on freq% and similarity% columns).

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
