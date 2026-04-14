"""
Signal analysis report formatter.

Renders analyze_dataset() results as a readable markdown report
for terminal display or file output.
"""


# All available report sections
ALL_SECTIONS = {"quality", "tokens", "stopwords", "aliases", "unicode", "suggestions"}


def format_report(results: dict, file_path: str = "",
                  columns: list | None = None,
                  sections: set | None = None,
                  top_n: int | None = 15) -> str:
    """Format analysis results as markdown. top_n limits items per section (None = all)."""
    if sections is None:
        sections = ALL_SECTIONS
    lines = []
    lines.append("# Signal Analysis Report")
    if file_path:
        lines.append(f"\n**Source:** {file_path}")
    if columns:
        lines.append(f"**Columns analyzed:** {', '.join(columns)}")
    lines.append("")

    # Data quality summary
    quality = results.get("data_quality", {})
    if quality and "quality" in sections:
        lines.append("## Data Quality")
        lines.append("")
        lines.append("| Column | Rows | Null % | Unique % | Duplicates |")
        lines.append("|---|---|---|---|---|")
        for col, q in quality.items():
            lines.append(
                f"| {col} | {q['total_rows']} | {q['null_pct']}% "
                f"| {q['unique_pct']}% | {q['duplicate_count']} |"
            )
        lines.append("")

    # Per-column analysis
    for col_name, data in results.get("columns", {}).items():
        col_type = data.get("detected_type", "unknown")
        lines.append(f"## {col_name} (detected: {col_type})")
        lines.append("")

        # Top tokens
        if "tokens" in sections:
            raw_tokens = data.get("top_tokens_raw", [])[:top_n]
            clean_tokens = data.get("top_tokens_clean", [])[:top_n]

            if raw_tokens:
                lines.append("**Top tokens (raw):**")
                lines.append("")
                lines.append("| Token | Count |")
                lines.append("|---|---|")
                for token, count in raw_tokens:
                    lines.append(f"| {token} | {count} |")
                lines.append("")

            if clean_tokens:
                lines.append("**Top tokens (clean):**")
                lines.append("")
                lines.append("| Token | Count |")
                lines.append("|---|---|")
                for token, count in clean_tokens:
                    lines.append(f"| {token} | {count} |")
                lines.append("")

        # Suggested stopwords
        if "stopwords" in sections:
            stopwords = data.get("suggested_stopwords", [])
            if stopwords:
                lines.append("**Suggested stopwords:**")
                lines.append("")
                lines.append("| Token | Frequency | Known |")
                lines.append("|---|---|---|")
                for sw in stopwords:
                    known = "yes" if sw.get("known") else ""
                    lines.append(
                        f"| {sw['token']} | {sw['frequency']:.0%} | {known} |"
                    )
                lines.append("")

        # Suggested aliases
        if "aliases" in sections:
            alias_groups = data.get("suggested_aliases", [])
            if alias_groups:
                lines.append(f"**Alias groups:** {len(alias_groups)} detected")
                lines.append("")
                show_aliases = alias_groups[:top_n] if top_n else alias_groups
                for ag in show_aliases:
                    variants = [f"{v['raw']} ({v['count']})" for v in ag["variants"]]
                    lines.append(f"- **{ag['canonical']}** -- {', '.join(variants)}")
                if top_n and len(alias_groups) > top_n:
                    lines.append(f"- ... and {len(alias_groups) - top_n} more")
                lines.append("")

        # Unicode profile
        if "unicode" not in sections:
            continue
        up = data.get("unicode_profile")
        if up and up.get("bucket_totals"):
            bt = up["bucket_totals"]
            total_chars = sum(bt.values())
            if total_chars > 0:
                lines.append("**Character ranges:**")
                lines.append("")
                lines.append("| Range | Characters | % |")
                lines.append("|---|---|---|")
                for rng, count in sorted(bt.items(), key=lambda x: -x[1]):
                    pct = round(count / total_chars * 100, 1)
                    lines.append(f"| {rng} | {count:,} | {pct}% |")
                lines.append("")

            # Unknown chars + mixed scripts
            has_unknown = up.get("cells_with_unknown", 0) > 0
            has_mixed = up.get("mixed_script_cells", 0) > 0
            if has_unknown or has_mixed:
                lines.append(
                    f"**Unicode flags:** {up.get('cells_with_unknown', 0)} cells with "
                    f"unknown characters ({up.get('cells_with_unknown_pct', 0)}%), "
                    f"{up.get('mixed_script_cells', 0)} mixed script"
                )
                lines.append("")

            # Non-ASCII sample rows
            non_ascii_buckets = {
                k for k in bt
                if k not in ("ascii_alnum", "ascii_punct_space") and bt[k] > 0
            }
            if non_ascii_buckets and col_name in results.get("_raw_series", {}):
                raw_series = results["_raw_series"][col_name]
                sample_rows = []
                for idx, val in enumerate(raw_series):
                    if val is None:
                        continue
                    s = str(val)
                    if any(ord(c) > 127 for c in s):
                        sample_rows.append((idx, s))
                    if len(sample_rows) >= 20:
                        break
                if sample_rows:
                    total_non_ascii = sum(
                        1 for v in raw_series
                        if v is not None and any(ord(c) > 127 for c in str(v))
                    )
                    lines.append(
                        f"**Non-ASCII rows:** {total_non_ascii} total "
                        f"(ranges: {', '.join(sorted(non_ascii_buckets))})"
                    )
                    lines.append("")
                    lines.append("| Row | Value |")
                    lines.append("|---|---|")
                    for idx, val in sample_rows:
                        lines.append(f"| {idx + 1} | {val[:80]} |")
                    if total_non_ascii > 20:
                        lines.append(
                            f"| ... | {total_non_ascii - 20} more rows |"
                        )
                    lines.append("")

    # Aggregated suggestions
    agg_sw = results.get("aggregated_stopwords", {})
    agg_al = results.get("aggregated_aliases", {})

    if (agg_sw or agg_al) and "suggestions" in sections:
        lines.append("## Aggregated Suggestions")
        lines.append("")
        lines.append(
            "These are the combined suggestions across all analyzed columns. "
            "Review and curate before saving to config."
        )
        lines.append("")

        if agg_sw:
            lines.append("**Stopwords (by column type):**")
            lines.append("")
            for col_type, words in agg_sw.items():
                lines.append(f"- **{col_type}:** {', '.join(words)}")
            lines.append("")

        if agg_al:
            lines.append("**Aliases:**")
            lines.append("")
            for variant, canonical in agg_al.items():
                lines.append(f"- {variant} -> {canonical}")
            lines.append("")

    return "\n".join(lines)
