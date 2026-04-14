"""CLI entry point for the relational matching pipeline.

Usage:
    python -m src --recipe config/recipes/l1_reconciliation.yaml
    python -m src --recipe config/recipes/l1_reconciliation.yaml --data data/ --output output/report.xlsx
    python -m src --recipe config/recipes/l1_reconciliation.yaml --no-libpostal
"""

import argparse
import sys
import time
from pathlib import Path

# Add src/ to path so bare imports (from normalize import ...) work
sys.path.insert(0, str(Path(__file__).parent))


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="relational_matching",
        description="Config-driven relational matching engine. "
        "Runs a recipe against source datasets and generates an Excel report.",
    )
    parser.add_argument(
        "--recipe",
        default="config/recipes/l1_reconciliation.yaml",
        help="Path to recipe YAML/JSON (default: config/recipes/l1_reconciliation.yaml)",
    )
    parser.add_argument(
        "--data",
        default="data",
        help="Base directory for data files referenced in the recipe (default: data)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Override output report path (default: from recipe config)",
    )
    parser.add_argument(
        "--no-libpostal",
        action="store_true",
        help="Force built-in address tokenizer even if libpostal is installed",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate recipe and load data without running the matching pipeline",
    )

    args = parser.parse_args()

    # Validate recipe path exists
    recipe_path = Path(args.recipe)
    if not recipe_path.exists():
        print(f"Error: recipe not found: {recipe_path}", file=sys.stderr)
        return 1

    # Validate data directory exists
    data_dir = Path(args.data)
    if not data_dir.is_dir():
        print(f"Error: data directory not found: {data_dir}", file=sys.stderr)
        return 1

    # Disable libpostal if requested
    if args.no_libpostal:
        import address
        address.LIBPOSTAL_AVAILABLE = False
        print("libpostal disabled — using built-in address tokenizer")

    from recipe import (
        load_recipe, validate_recipe, load_source, filter_population,
        build_filter_expr, validate_fields, format_validation_summary,
        RecipeValidationError,
    )
    from matching import run_pipeline
    from report import generate_report

    # Load and validate recipe
    print(f"Loading recipe: {recipe_path}")
    try:
        recipe = load_recipe(str(recipe_path))
    except (ValueError, FileNotFoundError) as e:
        print(f"\nError: {e}", file=sys.stderr)
        return 1

    # load_recipe already ran validate_recipe (raises on errors).
    # Call again to capture schema warnings for dry-run display.
    schema_warnings = validate_recipe(recipe)

    print(f"Recipe: {recipe.get('name', 'unnamed')}")
    print(f"Data directory: {data_dir}")

    if args.dry_run:
        # Enhanced dry-run: load data, build populations, validate fields
        sources = {}
        for name, cfg in recipe["sources"].items():
            sources[name] = load_source(cfg, str(data_dir))

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
            print("\n❌ Filter field errors:", file=sys.stderr)
            for e in filter_errors:
                print(f"  {e}", file=sys.stderr)
            return 1

        populations = {}
        for pop_name, pop_cfg in recipe["populations"].items():
            src_name = pop_cfg["source"]
            src_df = sources[src_name]
            if "filter" in pop_cfg and pop_cfg["filter"]:
                filtered = filter_population(src_df, pop_cfg)
                populations[pop_name] = {"config": pop_cfg, "df": filtered, "source": src_name}
            else:
                populations[pop_name] = {"config": pop_cfg, "df": None, "source": src_name}

        # Compute remainder populations
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
            for garb_name, garb_cfg in recipe["populations"].items():
                if garb_name == pop_name:
                    continue
                if garb_cfg.get("action") == "exclude" and "filter" in garb_cfg and garb_cfg["filter"]:
                    remainder = remainder.filter(~build_filter_expr(garb_cfg["filter"]))
            pop_data["df"] = remainder

        val_errors, val_warnings = validate_fields(recipe, sources, populations)
        summary = format_validation_summary(recipe, sources, populations, val_errors, val_warnings, schema_warnings)
        print(summary)
        return 1 if val_errors else 0

    print("Running matching pipeline...")
    t0 = time.time()
    try:
        result = run_pipeline(recipe, base_dir=str(data_dir))
    except RecipeValidationError as e:
        print(f"\nError: {e}", file=sys.stderr)
        print("Hint: run with --dry-run for detailed validation report", file=sys.stderr)
        return 1
    elapsed = time.time() - t0

    stats = result.get("stats", {})
    timing = result.get("timing", {})
    print(f"Pipeline complete in {elapsed:.2f}s")
    if timing:
        phases = [("load", "Load"), ("setup", "Setup"), ("match", "Match"), ("resolve", "Resolve")]
        parts = [f"{label} {timing[k]:.2f}s" for k, label in phases if k in timing]
        print(f"  Timing:            {' | '.join(parts)}")
    print(f"  Source records:    {stats.get('total_source', 'N/A')}")
    print(f"  Matched:           {stats.get('matched_count', 'N/A')}")
    print(f"  Unmatched:         {stats.get('unmatched_count', 'N/A')}")

    # Generate report
    output_path = args.output
    if output_path is None:
        from datetime import datetime as _dt
        recipe_name = recipe.get("name", "report").lower().replace(" ", "_")
        recipe_name = "".join(c if c.isalnum() or c == "_" else "" for c in recipe_name)
        timestamp = _dt.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"output/{recipe_name}_{timestamp}.xlsx"

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    t_report = time.time()
    report_path = generate_report(
        result["matched"],
        result["unmatched"],
        output_path,
        stats=stats,
        recipe=recipe,
    )
    print(f"Report saved: {report_path} ({time.time() - t_report:.2f}s)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
