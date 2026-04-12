# Benchmark Results - ADR-001 Tooling Validation

**Generated:** 2026-04-11
**Hardware:** 8 cores (throttled to emulate ZBook Ryzen 9 PRO 7940HS)
**Dataset:** 15,000 source x 500 target rows (seed=42)

[Test Case Definitions](../test_cases.md) | [ADR-001](../../docs/adr/001-matching-engine-tooling.md) | [README](../../README.MD)

---

## TC-01: Fuzzy Name Matching

| Option | Time | Rows | Memory | Matches |
|---|---|---|---|---|
| A | 29.869s (1,000) -> **448.0s projected** | 1,000 | 0.1 MB | 888 |
| B | 1.069s (1,000) -> **16.0s projected** | 1,000 | 0.2 MB | 932 |
| C | **0.185s** | 15,000 | 2.9 MB | 13149 |

## TC-02: Clean Normalization + Exact Match

| Option | Time | Rows | Memory | Matches |
|---|---|---|---|---|
| A | 0.002s (1,000) -> **0.0s projected** | 1,000 | 0.1 MB | 719 |
| B | 0.007s (1,000) -> **0.1s projected** | 1,000 | 0.4 MB | 719 |
| C | **0.006s** | 15,000 | 0.0 MB | 10450 |

## TC-03: Address Token Overlap

| Option | Time | Rows | Memory |
|---|---|---|---|
| A | 6.348s (1,000) -> **95.2s projected** | 1,000 | 0.0 MB |
| B | 42.191s (1,000) -> **632.9s projected** | 1,000 | 0.2 MB |
| C | **1.213s** | 15,000 | 0.0 MB |

**Score Distribution (Option C, 15k rows):**

| Range | Count |
|---|---|
| 0-59% | 314 |
| 60-79% | 4,211 |
| 80-99% | 4,492 |
| 100% | 5,983 |

## TC-04: Combined Pipeline (Date Gate + Name + Address)

| Option | Time | Rows | Memory | Matches | Dest. Filtered |
|---|---|---|---|---|---|
| A | 23.652s (1,000) -> **354.8s projected** | 1,000 | 0.1 MB | 846 | 101 |
| B | 1.044s (1,000) -> **15.7s projected** | 1,000 | 0.2 MB | 894 | 101 |
| C | **0.225s** | 15,000 | 2.9 MB | 12548 | 101 |

---

## Speedup Summary (TC-01, projected to 15k)

| Comparison | Result |
|---|---|
| A -> C | **2,422x faster** |
| B -> C | **86x faster** |

---

## Success Criteria

| # | Criteria | Result | Status |
|---|---|---|---|
| 1 | Option C total time < 60s | 1.63s | **PASS** |
| 2 | C >= 10x faster than B (TC-01) | 86x | **PASS** |
| 3 | C >= 100x faster than A (TC-01) | 2,422x | **PASS** |
| 4 | Match quality within 10% | 1.3% diff | **PASS** |
| 5 | Peak memory < 1GB | 2.9 MB | **PASS** |

---

*Options A and B were capped at 1,000 rows with projected extrapolation. Option C ran on the full 15,000 rows.*