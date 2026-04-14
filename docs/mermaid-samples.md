# Mermaid Diagram Samples -- L1 Reconciliation

All of these would be auto-generated from the recipe YAML. Review and tell me which direction works (or what to mix/match).

---

## Option A: Ultra-Simple (just the flow)

The absolute minimum -- data in, steps, data out.

```mermaid
flowchart LR
    A[36 Pop1 Records] --> B{Matching Steps}
    B --> C[32 Matched]
    B --> D[4 Unmatched]
```

---

## Option B: Steps Visible (linear)

Shows each step as a box in sequence with cascade.

```mermaid
flowchart TD
    Input[Pop1: 36 records] --> S1
    S1[Step 1: Exact to core_parent] --> S2
    S2[Step 2: Exact to Pop3] --> S3
    S3[Step 3: Fuzzy to core_parent] --> S4
    S4[Step 4: Fuzzy to Pop3] --> Out

    S1 -->|30 matched| Matched[32 Matched]
    S3 -->|2 matched| Matched
    Out[Remaining] -->|4| Unmatched[4 Unmatched]
```

---

## Option C: Sources and Populations

Shows where data comes from before the matching steps.

```mermaid
flowchart TD
    subgraph Sources
        CP[core_parent_export.csv]
        MP[tp_multi_pop_dataset.csv]
    end

    subgraph Populations
        MP --> Pop1[Pop1: 36 records\nvendor_id starts with V7]
        MP --> Garbage[Garbage: excluded]
        MP --> Pop3[Pop3: remaining]
    end

    subgraph Matching
        Pop1 --> S1[Step 1: Exact to core_parent]
        Pop1 --> S2[Step 2: Exact to Pop3]
        Pop1 --> S3[Step 3: Fuzzy to core_parent]
        Pop1 --> S4[Step 4: Fuzzy to Pop3]
        CP --> S1
        CP --> S3
        Pop3 --> S2
        Pop3 --> S4
    end

    S1 --> Result[32 Matched / 4 Unmatched]
    S2 --> Result
    S3 --> Result
    S4 --> Result
```

---

## Option D: Cascade Flow (shows the waterfall)

Emphasizes how records flow through steps -- the cascade behavior.

```mermaid
flowchart TD
    Pop1[Pop1: 36 records] --> S1[Step 1: Exact to core_parent]
    S1 -->|30 matched| M[Matched: 32]
    S1 -->|6 unmatched| S2[Step 2: Exact to Pop3]
    S2 -->|0 matched| M
    S2 -->|6 unmatched| S3[Step 3: Fuzzy to core_parent]
    S3 -->|2 matched| M
    S3 -->|4 unmatched| S4[Step 4: Fuzzy to Pop3]
    S4 -->|0 matched| M
    S4 -->|4 unmatched| U[Unmatched: 4]
```

---

## Option E: Simple with Config Details

Compact but shows key thresholds.

```mermaid
flowchart TD
    Pop1[Pop1: 36 records] --> S1

    S1["Step 1: Exact to core_parent
    addr >= 60% | updated < 2yr"]
    S1 -->|30| Matched

    S1 -.->|6 remaining| S2
    S2["Step 2: Exact to Pop3
    addr >= 60% | contract < 2yr"]
    S2 -.->|6 remaining| S3

    S3["Step 3: Fuzzy to core_parent
    name >= 70% | addr >= 60%"]
    S3 -->|2| Matched

    S3 -.->|4 remaining| S4
    S4["Step 4: Fuzzy to Pop3
    name >= 70% | addr >= 60%"]
    S4 -.->|4 remaining| Unmatched

    Matched[Matched: 32]
    Unmatched[Unmatched: 4]
```

---

## Option F: Bare Bones (almost a legend)

Just shapes and labels, nothing else.

```mermaid
flowchart LR
    Pop1((Pop1)) --> S1[Exact] --> S2[Exact] --> S3[Fuzzy] --> S4[Fuzzy] --> U((Unmatched))
    S1 --> M((Matched))
    S2 --> M
    S3 --> M
    S4 --> M
```

---

## My Take

**Option D** (cascade flow) is the most useful -- it's the one concept people struggle with and it makes it immediately visual. It's also the easiest to auto-generate since it just needs step names and counts.

**Option E** adds the thresholds which ties into the summary table but might be too busy.

**Option B** is a good middle ground if D feels too detailed.

What direction resonates?
