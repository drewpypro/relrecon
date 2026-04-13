# Relational Reconciliation Engine

A config-driven framework for reconciling datasets with broken or unreliable hierarchical relationships. Define a recipe, run the engine, get an auditable Excel report.

## Why This Exists

When vendor records get bulk-migrated between systems, parent-child relationships often break — parent IDs get populated with placeholder data, dates are invalid, and the downstream data is effectively untrustworthy. You're left with thousands of records that need reconciliation against trusted sources, and no reliable way to automate it.

This framework was built to solve that problem. Rather than writing a one-off script for each reconciliation task, the engine is **config-driven**: define your sources, populations, matching steps, and output format in a recipe YAML, and the engine handles normalization, matching, address scoring, and report generation.

The first recipe (**L1 Reconciliation**) reconciles ~15k migrated vendor records to trusted parent IDs. The framework is designed so future reconciliation problems only need a new recipe — not new code.

## Quickstart

```bash
# Clone
git clone https://git.drewpy.pro/drewpypro/relational_matching.git
cd relational_matching

# Install dependencies
pip install -r requirements.txt

# Run the pipeline against included synthetic data
python3 -m src --recipe config/recipes/l1_reconciliation.yaml --data data/

# Output: output/l1_reconciliation_report.xlsx (Matched + Analysis tabs)

# Other options
python3 -m src --help                       # See all flags
python3 -m src --recipe ... --dry-run       # Validate recipe without running
python3 -m src --recipe ... --no-libpostal  # Force built-in address tokenizer

# Run tests
python3 -m pytest tests/ -q
```

> **Note:** libpostal is optional. The engine includes a built-in address tokenizer that works without any external dependencies. Install libpostal for better accuracy on international/complex addresses — see the [install guide](docs/implementation-plan.md#libpostal-installation).

## How It Works

The engine has five core modules, all driven by a single recipe config:

| Module | Purpose |
|---|---|
| **normalize.py** | 3-tier normalization: Raw (as-is), Clean (lowercase, strip punctuation), Normalized (aliases + stopwords, addresses only) |
| **signal_analysis.py** | Profile datasets to bootstrap stopwords, aliases, and unicode maps before configuring a recipe |
| **address.py** | Two-pass address parsing + scoring with optional libpostal. Merges and cross-compares address fields, weights street name as strongest signal |
| **matching.py** | Core engine — loads populations, applies date gates, runs multi-step name matching, scores addresses, resolves multi-match conflicts |
| **report.py** | Excel output with Matched tab (derived L1, scores, validation data) and Analysis tab (unmatched records with reason codes) |

### Recipes

A recipe defines **what** to match. The engine handles **how**. Here's the structure of the included L1 recipe ([full YAML](config/recipes/l1_reconciliation.yaml)):

```yaml
name: L1 Reconciliation

sources:
  core_parent:
    file: core_parent_export.csv
    type: trusted_reference
  multi_pop:
    file: tp_multi_pop_dataset.csv
    type: multi_population

populations:
  pop1:                         # Target — records with broken parent IDs
    source: multi_pop
    filter:
      - field: vendor_id
        op: starts_with
        value: "V7"
  garbage:                      # Exclude — automated migration junk
    source: multi_pop
    filter: [...]
    action: exclude
  pop3:                         # Valid — everything remaining
    source: multi_pop
    filter: []

steps:
  - name: Match Pop1 to core_parent
    source: pop1
    destination: core_parent
    match_fields:
      - source: l3_fmly_nm     # Match child (L3) names
        destination: Vendor Name
        method: exact
        tiers: [raw, clean]     # Try exact first, then cleaned
    address_support:            # Address scoring as supporting evidence
      source: [hq_addr1, hq_addr2]
      destination: [Address1, Address2]
    date_gate:                  # Only match against recent records
      field: Updated
      max_age_years: 2
    inherit:                    # On match, derive the parent (L1)
      - source: Supplier Name
        as: derived_l1_name
      - source: Supplier ID
        as: derived_l1_id

  - name: Match Pop1 to Pop3   # Second pass against valid population
    # ... same structure, different fields

output:
  format: xlsx
  match_mode: best_match        # or all_matches for analysis
  columns:                      # Optional — define report columns per tab
    matched:
      - field: l3_fmly_nm
        header: Source L3 Name
      - field: derived_l1_name
        header: Derived L1 Name
      - field: match_step
        header: Match Source
      - fields: [Vendor Name, l3_fmly_nm_dst]  # Coalesce variants from different steps
        header: Dest L3 Name
      # ... full column list in config/recipes/l1_reconciliation.yaml
    analysis:
      - field: l3_fmly_nm
        header: L3 Name
      # ...
```

To create a new recipe for a different reconciliation problem, copy this YAML and adjust the sources, populations, field mappings, and matching steps. The engine code stays the same.

### Report Columns

The `output.columns` section is **optional**. When omitted, the engine uses built-in defaults that match the L1 recipe. When defined, it controls exactly which columns appear in the Excel report and in what order.

Each entry maps a DataFrame column (`field`) to a display header (`header`). For destination columns that have different names depending on which matching step produced them, use `fields` (plural) to list the variants — the engine automatically coalesces them:

```yaml
# Single column
- field: vendor_id
  header: Vendor ID

# Variant columns (coalesced — picks the first non-null)
- fields: [Vendor Name, l3_fmly_nm_dst]
  header: Dest L3 Name
```

This is how you include columns in the report that aren't used in matching — just add them to `output.columns`. The engine carries all source columns through the pipeline; `output.columns` controls which ones surface in the report.

### Normalization Tiers

| Tier | Transform | Used For | Confidence |
|---|---|---|---|
| **Raw** | None — compare as-is | Names + Addresses | Highest |
| **Clean** | Lowercase, strip whitespace and punctuation | Names + Addresses | Moderate |
| **Normalized** | Clean + alias replacement + stopword removal | **Addresses only** | Lowest |

Matching tries Raw first, then Clean. First match wins. The Normalized tier is configurable per recipe — the `tiers` field in each matching step controls which tiers are applied.

> [!IMPORTANT]
> **For this recipe, names are not normalized.** Suffixes like Inc/Ltd/Pty Ltd distinguish different entities and L1 parents in this dataset.
> Example: "Armitage Solutions Pty Ltd" (V562841 → S07589) vs "Armitage Solutions Ltd" (V562900 → S07601) are **different L1 parents** that normalization would incorrectly merge.
> Other recipes may benefit from name normalization — add `normalized` to the `tiers` list in the recipe's match_fields to enable it.

### Signal Analysis

Before configuring a recipe, the engine can profile source data to generate normalization config:

- **Top N tokens** per column at Raw and Clean tiers
- **Auto-detect column type** — name, address, date, ID
- **Suggested stopwords** — high-frequency, low-signal tokens (e.g., "Inc", "LLC")
- **Suggested aliases** — tokens that collapse to the same form (e.g., "Blvd" / "Boulevard")
- **Unicode profiling** — flag cells with unexpected scripts, fullwidth characters
- **Data quality summary** — null %, unique %, duplicate patterns

Output feeds directly into recipe config (`config/stopwords.json`, `config/aliases.json`). Signal analysis is currently invoked programmatically — see `src/signal_analysis.py`.

> [!IMPORTANT]
> Signal analysis and the matching engine **share the same normalization functions** (`normalize.py`). This ensures generated stopwords/aliases align with how data is actually compared during matching.

## Documentation

| Document | Purpose |
|---|---|
| **README** (this file) | What this is, how to use it, framework overview, L1 recipe details |
| **[ADR-001](docs/adr/001-matching-engine-tooling.md)** | Why this tech stack (Polars + RapidFuzz + libpostal). Four options benchmarked. |
| **[Implementation Plan](docs/implementation-plan.md)** | How it was built — 5 phases, technical decisions, PR history, libpostal install guide |

## The First Recipe: L1 Reconciliation

Everything below describes the **first recipe** built on this framework — the real-world problem that motivated building it. The framework itself (normalization, signal analysis, matching engine, reporting) is reusable. Only the recipe config changes between use cases.

### The Problem

A large population of vendor records (~15k rows, 232 columns) was migrated into a source-of-record system over the past decade. During migration, parent-child relationships broke:

- **Parent IDs (`tpty_l1_id`)** were populated with placeholder or incorrect values
- **Parent names (`l1_fmly_nm`)** don't match the actual parent entities
- **Contract dates (`cntrct_cmpl_dt`)** are invalid for the migrated population

The only reliable field for the migrated records is the **child/vendor name (`l3_fmly_nm`)**. The goal: use that name to find the correct parent by matching against trusted sources and inheriting the real parent relationship.

### Terminology

| Term | Meaning |
|---|---|
| **L1** | Parent level (Supplier). What we're trying to derive. |
| **L3** | Child/Vendor level. The name we match on. |
| **Pop1** | Target population — migrated records with broken L1 fields (vendor_id starts with V7) |
| **Pop3** | Valid population — remaining records from same dataset, not guaranteed clean |
| **core_parent** | Trusted reference dataset with reliable L1↔L3 relationships |
| **Date gate** | A filter that excludes destination records older than 2 years |

### Field Mapping

| Level | core_parent_export | tp_multi_pop_dataset |
|---|---|---|
| L3 (Child/Vendor) | Vendor Name, Vendor ID | l3_fmly_nm, vendor_id |
| L1 (Parent/Supplier) | Supplier Name, Supplier ID | l1_fmly_nm, tpty_l1_id |
| Address | Address1, Address2 | hq_addr1, hq_addr2 |

> [!IMPORTANT]
> Pop1's `l1_fmly_nm`, `tpty_l1_id`, and `cntrct_cmpl_dt` are all **invalid** (migrated with bad data). Do not use any of these fields for matching or filtering.
> The goal is to derive correct L1 by matching L3 names against trusted sources and inheriting the parent relationship.

### Datasets

**core_parent_export.csv** (Trusted Reference)
- Master vendor/supplier database with recent, trusted data
- Key columns: Updated (date), Vendor Name/ID (L3), Supplier Name/ID (L1), Address1/Address2

**tp_multi_pop_dataset.csv** (Multi-Population Source)
- Contains 3 populations in one file, separated by filter logic:

| Population | Filter | Description |
|---|---|---|
| Pop1 (target) | `vendor_id LIKE V7%` | Target for reconciliation — invalid L1 fields |
| Garbage | non-V7 + `data_entry_type = 'Migrated'` + `rq_intk_user` contains "Data Migration" or "Goblindor" | Excluded from matching |
| Pop3 (valid) | Everything remaining | Valid records from real users — not guaranteed clean |

> [!CAUTION]
> Pop3 may still contain data quality issues. It simply passed the garbage filter. This is why exact name matching is required rather than trusting Pop3 fields at face value.

<details>
<summary><b>Key Column Details</b></summary>

- **l3_fmly_nm** — Child/Vendor name. The primary matching field.
- **tpty_assm_nm** — Infosec assessment third-party name. Better data quality when populated. Always included in report output for manual review.
- **cntrct_cmpl_dt** — Contract completion date. Invalid for Pop1. Used for 2-year recency on destination records only.
- **hq_addr1, hq_addr2** — Address fields. Mixed quality — data may be in either column, split across both, or abbreviated.
- **data_entry_type** — `Migrated` (from another system) or `net_new` (generated through normal business processes).
- **rq_intk_user** — Identifies automated migrations (Data Migration, Goblindor) vs. normal users.
- **Other columns** (timestamp, cntrc_expt, obgr_mgr_ext, tep_dgr, ogr_mdi) — informational only, not used for matching.

</details>

### Context

- Data migration between systems is a normal process, but over the past decade, large migrations occurred under time pressure with inadequate tooling
- As migrations progressed they improved, but early batches lacked reconciliation back to trusted records — some fields were populated with best-effort or placeholder values
- Pop1 records (all V7* vendor IDs) were migrated by Data Migration or Goblindor users with invalid `l1_fmly_nm`, `tpty_l1_id`, and `cntrct_cmpl_dt`

### Matching Flow

**Step 1: Pop1 → core_parent** — Match `l3_fmly_nm` ↔ `Vendor Name` (Raw/Clean exact). On match, inherit L1 (Supplier Name/ID). Destination `Updated` must be within 2 years.

**Step 2: Pop1 → Pop3** — Match `l3_fmly_nm` ↔ `l3_fmly_nm` (Raw/Clean exact). On match, inherit `tpty_l1_id`/`l1_fmly_nm`. Pop3's `cntrct_cmpl_dt` must be within 2 years.

Both steps run against **all** Pop1 records. In `best_match` mode (default), core_parent wins when both steps match the same record. Use `all_matches` mode in the recipe to see all candidates for analysis.

Address matching is **supporting evidence** in both steps — not standalone match criteria.

### Address Matching

<details>
<summary><b>How address matching works (detailed walkthrough)</b></summary>

**Problem:** Address data is split unpredictably across two columns. We can't trust which column holds what.

**Approach:**

1. **Build variants** — For each record: `addr1_only`, `addr2_only`, `addr_merged` (concatenated)
2. **Parse** — Extract street name (key signal), city, state, zip. Uses libpostal if available, built-in tokenizer otherwise.
3. **Normalize** — Raw → Clean → Normalized (aliases expanded)
4. **Compare** — merged↔merged, addr1↔addr1, addr1↔addr2, addr2↔addr1 — best score wins
5. **Score** — Token overlap % weighted by component. Street name is boosted (60% weight when detected).

**Example:**
```
Pop1:  "194 6th Avenue Floor 7"        → street: 6th Avenue
Core:  "194 6TH AVENUE FL 7 NY 10005"  → street: 6th Avenue

Street match: 100% | Token overlap: 67% | Weighted: ~85%
```

**Key insight:** 60% overall + street match = strong signal. 90% overall with no street match = suspicious (matching on city/state/zip only).

</details>

### Date Rules

- 2-year recency window applies to **destination records only**:
    - core_parent: `Updated` must be within 2 years
    - Pop3: `cntrct_cmpl_dt` must be within 2 years
- Pop1's `cntrct_cmpl_dt` is invalid and is NOT checked

### Output Report

Excel workbook with two tabs:

- **Matched** — Source/destination L3 names (side-by-side), **derived L1 ID + Name**, match source, match tier, address scores with source and destination addresses, and `tpty_assm_nm` for review
- **Analysis** — Unmatched Pop1 records with reason codes for human review

### Constraints

- **Dataset is global** — addresses span multiple countries/formats (not US-only)
- Must handle hierarchical relationships (L1 ↔ L3, vendor ↔ parent)
- Address field quality varies (addr2 unreliable); prioritize addr1
- 2-year recency window on destination records only (Pop1 dates are invalid)
- Must distinguish between 3 populations in the multi-pop dataset
- Pop3 is not guaranteed clean — exact matching required
- Report must be auditable (show *why* each match fired)
- Must run on local developer hardware (HP ZBook: Ryzen 9 PRO 7940HS 8-core, 64GB RAM)

## Project Status

All implementation phases complete. See [Implementation Plan](docs/implementation-plan.md) for PR history and technical decisions.

- [x] Synthetic datasets
- [x] ADR: Tooling decision ([ADR-001](docs/adr/001-matching-engine-tooling.md))
- [x] Signal analysis module
- [x] Address handling (libpostal optional, built-in fallback)
- [x] Matching engine (config-driven, recipe-based)
- [x] Report generation (Excel with formatting)
- [x] CLI entry point (`python3 -m src --recipe ...`)
- [x] Documentation

**Stack:** Python + Polars + RapidFuzz + openpyxl + libpostal (optional). See [ADR-001](docs/adr/001-matching-engine-tooling.md) for why.

**Deployment:** Currently local development. Designed to be portable — no hardcoded paths, no OS-specific dependencies beyond Python and optionally libpostal's C library.
