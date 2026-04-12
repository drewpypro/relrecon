# Benchmarks

Tooling benchmark comparing [ADR-001](../docs/adr/001-matching-engine-tooling.md) Options A/B/C on realistic synthetic data.

## Purpose

Validate the performance claims in ADR-001 with measurable results before accepting the tooling decision.

## Structure

```
benchmarks/
├── README.md               ← this file
├── test_cases.md           ← test case definitions (inputs, expected outputs)
├── generate_data.py        ← generates synthetic 15k-row dataset
├── bench_option_a.py       ← Option A: difflib + iterrows
├── bench_option_b.py       ← Option B: pandas + fuzzywuzzy
├── bench_option_c.py       ← Option C: polars + rapidfuzz
├── run_all.py              ← orchestrator: runs all options, writes results
└── results/                ← output directory (gitignored)
    ├── summary.json        ← timing, memory, match counts per option
    └── summary.txt         ← human-readable summary
```

## Usage

```bash
# Generate synthetic data
python benchmarks/generate_data.py

# Run all benchmarks
python benchmarks/run_all.py

# View results
cat benchmarks/results/summary.txt
```

## Hardware Throttling

All benchmarks throttle to 8 cores (`POLARS_MAX_THREADS=8`, `os.sched_setaffinity` where supported) to emulate the HP ZBook Ryzen 9 PRO 7940HS.
