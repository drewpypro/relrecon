"""
Recipe config loader and validator.

Loads YAML/JSON recipes, validates structure, resolves data sources,
and parses the filter DSL into Polars expressions.
"""

import json
from pathlib import Path
from typing import Optional

import polars as pl

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


def load_recipe(path: str) -> dict:
    """Load a recipe from YAML or JSON file."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Recipe not found: {path}")

    with open(p) as f:
        if p.suffix in (".yaml", ".yml"):
            if not YAML_AVAILABLE:
                raise ImportError("PyYAML required for YAML recipes: pip install pyyaml")
            recipe = yaml.safe_load(f)
        else:
            recipe = json.load(f)

    validate_recipe(recipe)
    return recipe


def validate_recipe(recipe: dict) -> None:
    """Validate recipe structure. Raises ValueError on problems."""
    required = ["name", "sources", "populations", "steps", "output"]
    missing = [k for k in required if k not in recipe]
    if missing:
        raise ValueError(f"Recipe missing required fields: {missing}")

    for name, src in recipe["sources"].items():
        if "file" not in src:
            raise ValueError(f"Source '{name}' missing 'file' field")

    for i, step in enumerate(recipe["steps"]):
        for k in ["name", "source", "destination", "match_fields"]:
            if k not in step:
                raise ValueError(f"Step {i} ('{step.get('name', '?')}') missing '{k}'")

    if "format" not in recipe["output"]:
        raise ValueError("Output missing 'format' field")

    # Warn if a source population used in steps is missing record_key.
    # Only check populations that appear as a step source (not destinations
    # like pop3, not excluded populations like garbage).
    source_pops = {step["source"] for step in recipe.get("steps", [])}
    for pop_name in source_pops:
        pop_cfg = recipe.get("populations", {}).get(pop_name, {})
        if pop_cfg.get("action") == "exclude":
            continue
        if "record_key" not in pop_cfg:
            import sys
            print(
                f'[WARN] Population "{pop_name}" has no record_key. '
                "Dedup will fall back to match field — records with "
                "duplicate names may be collapsed. Set record_key to the "
                "field that uniquely identifies each source record.",
                file=sys.stderr,
            )


def load_source(source_config: dict, base_dir: str = ".") -> pl.DataFrame:
    """Load a data source. Auto-detects CSV/Parquet from extension."""
    file_path = Path(base_dir) / source_config["file"]
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix == ".parquet":
        return pl.read_parquet(str(file_path))
    elif suffix in (".csv", ".tsv"):
        return pl.read_csv(str(file_path), infer_schema_length=0)
    else:
        raise ValueError(f"Unsupported format: {suffix}")


def build_filter_expr(filter_config: list, join_mode: str = "and") -> pl.Expr:
    """Build a Polars expression from the filter DSL.

    Each condition: {field, op, value/values}
    join_mode is set at filter list level (not per-condition).
    """
    exprs = []

    # Extract join_mode from any condition that declares it (legacy support)
    for cond in filter_config:
        if "join" in cond:
            join_mode = cond["join"]
            break

    for cond in filter_config:

        field = cond["field"]
        op = cond["op"]
        col = pl.col(field).cast(pl.String)

        if op == "eq":
            exprs.append(col == cond["value"])
        elif op == "neq":
            exprs.append(col != cond["value"])
        elif op == "starts_with":
            exprs.append(col.str.starts_with(cond["value"]))
        elif op == "not_starts_with":
            exprs.append(~col.str.starts_with(cond["value"]))
        elif op == "contains":
            exprs.append(col.str.contains(cond["value"], literal=True))
        elif op == "contains_any":
            sub = col.str.contains(cond["values"][0], literal=True)
            for v in cond["values"][1:]:
                sub = sub | col.str.contains(v, literal=True)
            exprs.append(sub)
        else:
            raise ValueError(f"Unknown filter op: {op}")

    if not exprs:
        return pl.lit(True)

    result = exprs[0]
    for e in exprs[1:]:
        result = result | e if join_mode == "or" else result & e
    return result


def filter_population(df: pl.DataFrame, pop_config: dict) -> pl.DataFrame:
    """Filter DataFrame by population config."""
    if "filter" not in pop_config or not pop_config["filter"]:
        return df
    return df.filter(build_filter_expr(pop_config["filter"]))


# ---------------------------------------------------------------------------
# Semantic field validation
# ---------------------------------------------------------------------------

class RecipeValidationError(Exception):
    """Raised when semantic validation finds critical field errors."""
    pass


def validate_fields(
    recipe: dict,
    sources: dict[str, pl.DataFrame],
    populations: dict[str, dict],
) -> tuple[list[str], list[str]]:
    """Validate all recipe field references against loaded DataFrames.

    Args:
        recipe: Parsed recipe dict
        sources: {name: DataFrame} from loaded CSV/Parquet files
        populations: {name: {config, df, source}} after filtering

    Returns:
        (errors, warnings) — errors are critical (match_fields, inherit),
        warnings are non-fatal (address_support, date_gate, filter).
    """
    errors = []
    warnings = []

    def _check(field: str, df: pl.DataFrame, context: str, critical: bool = True):
        if field not in df.columns:
            available = ", ".join(sorted(df.columns)[:10])
            msg = f'{context}: field "{field}" not found. Available: {available}'
            if critical:
                errors.append(msg)
            else:
                warnings.append(msg)

    # Validate population filter fields
    for pop_name, pop_cfg in recipe["populations"].items():
        src_name = pop_cfg.get("source", "")
        if src_name not in sources:
            continue  # Source loading would have already failed
        src_df = sources[src_name]
        for cond in pop_cfg.get("filter", []):
            if "field" in cond:
                _check(
                    cond["field"], src_df,
                    f'Population "{pop_name}" filter',
                    critical=False,
                )

    # Validate step field references
    for i, step in enumerate(recipe["steps"]):
        step_label = f'Step {i+1} "{step.get("name", "?")}"'
        src_pop = step.get("source", "")
        dst_pop = step.get("destination", "")

        src_df = populations.get(src_pop, {}).get("df")
        dst_df = populations.get(dst_pop, {}).get("df")

        # If destination is a source (not a population), check sources
        if dst_df is None and dst_pop in sources:
            dst_df = sources[dst_pop]

        # match_fields (critical)
        for mf in step.get("match_fields", []):
            if src_df is not None:
                _check(mf["source"], src_df, f"{step_label} match_fields.source", critical=True)
            if dst_df is not None:
                _check(mf["destination"], dst_df, f"{step_label} match_fields.destination", critical=True)

        # address_support (warning)
        if "address_support" in step:
            ac = step["address_support"]
            for af in ac.get("source", []):
                if src_df is not None:
                    _check(af, src_df, f"{step_label} address_support.source", critical=False)
            for af in ac.get("destination", []):
                if dst_df is not None:
                    _check(af, dst_df, f"{step_label} address_support.destination", critical=False)

        # date_gate (warning)
        if "date_gate" in step:
            dg = step["date_gate"]
            dg_field = dg.get("field", "")
            applies_to = dg.get("applies_to", "destination")
            check_df = dst_df if applies_to == "destination" else src_df
            if check_df is not None and dg_field:
                _check(dg_field, check_df, f"{step_label} date_gate", critical=False)

        # inherit (critical)
        for inh in step.get("inherit", []):
            if dst_df is not None:
                _check(
                    inh["source"], dst_df,
                    f"{step_label} inherit",
                    critical=True,
                )

    # Validate output.columns field references
    # These are explicitly requested by the recipe author, so missing
    # fields are errors (not warnings) — the report will silently drop them.
    output_columns = recipe.get("output", {}).get("columns", {})
    # Collect all known source columns for validation
    all_source_cols: set[str] = set()
    for src_df in sources.values():
        all_source_cols.update(src_df.columns)
    for pop_data in populations.values():
        if pop_data["df"] is not None:
            all_source_cols.update(pop_data["df"].columns)
    # Known derived/metadata columns the pipeline creates
    # Static metadata columns always present in output
    known_derived = {
        "match_step", "match_tier", "name_score",
        "addr_score", "addr_street_match", "addr_comparison", "addr_tier",
    }
    # Dynamically add columns from recipe inherit[].as values
    for step in recipe.get("steps", []):
        for inh in step.get("inherit", []):
            if "as" in inh:
                known_derived.add(inh["as"])

    for tab_key in ("matched", "analysis"):
        for i, entry in enumerate(output_columns.get(tab_key, [])):
            if "field" not in entry and "fields" not in entry:
                errors.append(
                    f'output.columns.{tab_key}[{i}]: entry must have '
                    f'either "field" or "fields"'
                )
                continue
            if "field" in entry and "fields" in entry:
                errors.append(
                    f'output.columns.{tab_key}[{i}]: entry has both '
                    f'"field" and "fields" — use one or the other'
                )
            if "field" in entry:
                f = entry["field"]
                if f not in all_source_cols and f not in known_derived:
                    errors.append(
                        f'output.columns.{tab_key}: field "{f}" not found in '
                        f"source data or known derived columns"
                    )
            if "fields" in entry:
                for f in entry["fields"]:
                    # Variant fields include _dst suffixed cols which won't
                    # exist until after the join — only warn on base names
                    if not f.endswith("_dst") and f not in all_source_cols:
                        warnings.append(
                            f'output.columns.{tab_key}: variant field "{f}" '
                            f"not found in source data"
                        )

    return errors, warnings


def format_validation_summary(
    recipe: dict,
    sources: dict[str, pl.DataFrame],
    populations: dict[str, dict],
    errors: list[str],
    warnings: list[str],
) -> str:
    """Format a human-readable validation summary for --dry-run."""
    lines = []
    lines.append(f"Recipe: {recipe.get('name', 'unnamed')}")
    lines.append(f"Schema: ✅ valid")
    lines.append("")

    lines.append("Sources:")
    for name, df in sources.items():
        src_cfg = recipe["sources"][name]
        lines.append(f"  {name}: {src_cfg['file']} ({df.height} rows, {df.width} cols)")
    lines.append("")

    lines.append("Populations:")
    for pop_name, pop_data in populations.items():
        df = pop_data["df"]
        pop_cfg = pop_data["config"]
        row_count = df.height if df is not None else 0
        action = pop_cfg.get("action", "")
        label = f" (excluded)" if action == "exclude" else ""
        filters = pop_cfg.get("filter", [])
        if filters:
            filter_parts = []
            for f in filters:
                if "field" in f:
                    filter_parts.append(f"{f['field']} {f['op']} {f.get('value', f.get('values', ''))}")
            if filter_parts:
                label += f" (filter: {', '.join(filter_parts)})"
        lines.append(f"  {pop_name}: {row_count} rows{label}")
    lines.append("")

    lines.append("Field validation:")
    for i, step in enumerate(recipe["steps"]):
        step_label = f'Step {i+1} "{step.get("name", "?")}"'
        lines.append(f"  {step_label}:")

        src_pop = step.get("source", "")
        dst_pop = step.get("destination", "")
        src_df = populations.get(src_pop, {}).get("df")
        dst_df = populations.get(dst_pop, {}).get("df")
        if dst_df is None and dst_pop in sources:
            dst_df = sources[dst_pop]

        for mf in step.get("match_fields", []):
            s_ok = src_df is not None and mf["source"] in src_df.columns
            d_ok = dst_df is not None and mf["destination"] in dst_df.columns
            lines.append(f"    match_fields.source: {mf['source']} {'✅' if s_ok else '❌'}")
            lines.append(f"    match_fields.destination: {mf['destination']} {'✅' if d_ok else '❌'}")

        if "address_support" in step:
            ac = step["address_support"]
            for af in ac.get("source", []):
                ok = src_df is not None and af in src_df.columns
                lines.append(f"    address_support.source: {af} {'✅' if ok else '⚠️'}")
            for af in ac.get("destination", []):
                ok = dst_df is not None and af in dst_df.columns
                lines.append(f"    address_support.destination: {af} {'✅' if ok else '⚠️'}")

        if "date_gate" in step:
            dg = step["date_gate"]
            dg_field = dg.get("field", "")
            applies_to = dg.get("applies_to", "destination")
            check_df = dst_df if applies_to == "destination" else src_df
            ok = check_df is not None and dg_field in check_df.columns
            lines.append(f"    date_gate: {dg_field} {'✅' if ok else '⚠️'}")

        for inh in step.get("inherit", []):
            ok = dst_df is not None and inh["source"] in dst_df.columns
            lines.append(f"    inherit: {inh['source']} → {inh['as']} {'✅' if ok else '❌'}")

    lines.append("")

    if errors:
        lines.append(f"❌ {len(errors)} error(s):")
        for e in errors:
            lines.append(f"  {e}")
    if warnings:
        lines.append(f"⚠️  {len(warnings)} warning(s):")
        for w in warnings:
            lines.append(f"  {w}")

    if not errors and not warnings:
        lines.append("✅ All field references valid. Ready to run.")
    elif not errors:
        lines.append("\n✅ No critical errors. Ready to run (with warnings).")
    else:
        lines.append("\n❌ Critical errors found. Pipeline will not run.")

    return "\n".join(lines)
