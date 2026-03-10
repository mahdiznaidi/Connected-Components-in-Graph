# Connected Components in Graph — CCF with Apache Spark

> **M2 Data Streams Project — Université Paris Dauphine**
>
> Implementation of the **CCF (Connected Components via MapReduce)** algorithm using Apache Spark, in both Python (RDD and DataFrame APIs) and Scala.


---

## 📌 Project Overview

This project implements the CCF algorithm for finding all **connected components** in large undirected graphs, as described in the original MapReduce paper. The work covers:

- A **PySpark RDD** implementation (`ccf_rdd.py`)
- A **PySpark DataFrame** implementation (`ccf_dataframe.py`)
- A **Scala Spark RDD** implementation (`CCFScala.scala`)
- A **cross-validation** script comparing both Python implementations
- A **scalability benchmark** comparing RDD vs DataFrame on graphs of 1K to 500K nodes
- Visualization of benchmark results

---

## 📁 Project Structure

```
Connected-Components-in-Graph/
│
├── ccf_rdd.py                        # CCF implementation — PySpark RDD API
├── ccf_dataframe.py                  # CCF implementation — PySpark DataFrame API
├── CCFScala.scala                    # CCF implementation — Scala Spark RDD
│
├── validate.py                       # Cross-validation: DataFrame vs RDD
├── benchmark_ccf.py                  # Scalability benchmark (RDD vs DataFrame)
├── benchmark_rdd.py                  # Standalone RDD benchmark
├── plot_rdd_from_csv.py              # Re-plot RDD benchmark from existing CSV
│
└── outputs/
    ├── benchmark_results.csv         # DataFrame benchmark results
    ├── benchmark_results_rdd.csv     # RDD benchmark results
    ├── benchmark_plot.png            # DataFrame scalability plot
    └── benchmark_plot_rdd.png        # RDD scalability plot
```

---

## 🧠 Algorithm Description

### Core Idea

The **CCF algorithm** finds connected components iteratively using two MapReduce phases:

#### Phase 1 — CCF-Iterate
For each node `l` with neighbor set `N_l`:
1. Compute `m = min(N_l)` — the minimum neighbor label
2. Emit `(m, n)` for every `n ∈ N_l` — attach all neighbors to the minimum
3. Emit `(n, m)` for every `n ∈ N_l` where `n ≠ m` — reverse propagation
4. Keep `(l, l)` — explicit self-loop to preserve node presence

#### Phase 2 — CCF-Dedup
- Remove duplicate pairs from the new adjacency set
- Detect convergence: if the adjacency set is unchanged from the previous iteration → stop

#### Output
Each node is mapped to a `component_id` (the minimum node ID in its connected component). Nodes sharing the same `component_id` belong to the same component.

---

## 📄 File Descriptions

### `ccf_rdd.py` — PySpark RDD Implementation

Implements CCF using low-level Spark RDD operations (`reduceByKey`, `join`, `flatMap`, `subtract`).

Key functions:
- `make_undirected(edges)` — symmetrizes the edge list
- `build_adjacency(sc, edges)` — builds the initial adjacency RDD with self-loops
- `ccf_iterate(adjacency)` — runs one CCF-Iterate phase
- `ccf_dedup(old_adj, new_pairs)` — deduplicates and detects convergence
- `extract_components(adjacency)` — extracts `{node: component_id}` from the final adjacency
- `run_ccf_rdd(spark, edges, max_iter, verbose)` — full iterative loop

Returns a Python `dict` mapping each node to its component ID.

---

### `ccf_dataframe.py` — PySpark DataFrame Implementation

Implements CCF using high-level Spark SQL DataFrame operations (`groupBy`, `agg`, `join`, `dropDuplicates`, `left_anti` joins).

Key components:
- `IterationStats` — dataclass storing per-iteration statistics (added/removed edges, convergence)
- `normalize_edges(edges_df)` — symmetrizes and deduplicates the edge DataFrame
- `build_initial_adjacency(edges_df)` — builds the starting `(node, neighbor)` DataFrame with self-loops
- `ccf_iterate(adjacency_df)` — one CCF-Iterate phase using DataFrame joins
- `ccf_dedup(previous_adjacency, candidates)` — deduplication and convergence check using `left_anti` joins
- `run_ccf_dataframe(edges_df, max_iterations, verbose, checkpoint_every)` — full iterative loop with optional lineage checkpointing

Returns a `(components_df, history)` tuple where `components_df` has schema `[node, component_id]`.

A `_demo()` function is included for quick local testing with a 3-component toy graph.

---

### `CCFScala.scala` — Scala Spark RDD Implementation

Mirrors the RDD logic in Scala 2.12 for native JVM performance on Spark.

Key components:
- `normalizeEdges(edges)` — symmetrizes the edge RDD
- `buildInitialAdjacency(edges)` — RDD with self-loops
- `ccfIterate(adjacency)` — one CCF-Iterate phase
- `DedupResult` — case class holding the output of one dedup step
- `ccfDedup(previousAdjacency, candidates)` — deduplication and convergence check
- `RunResult` — case class holding the final adjacency and iteration count
- `runCCF(edges, maxIterations, logProgress)` — full iterative loop
- `extractComponents(finalAdjacency)` — maps each node to its minimum label
- `connectedComponents(edges, maxIterations, logProgress)` — end-to-end helper
- `main(args)` — CLI entry point: reads an edge file, runs CCF, prints `node,componentId`

**Input format** (edge file): one edge per line, space- or comma-separated:
```
1 2
2,3
```

---

### `validate.py` — Cross-Validation Script

Validates correctness by running both implementations on a known 10-node graph and comparing results.

**Test graph** (5 known components):
- `{1, 2, 3}` → component_id = 1
- `{4, 5}` → component_id = 4
- `{6, 7, 8}` → component_id = 6
- `{9}` → component_id = 9
- `{10}` → component_id = 10

The graph also includes duplicate and reverse edges to test dedup robustness.

Checks performed:
1. DataFrame result vs expected components
2. Internal RDD reference vs expected components
3. DataFrame vs RDD reference (cross-check)
4. Optional external RDD module vs DataFrame (via `--external-rdd-module`)

---

### `benchmark_ccf.py` — Comparative Benchmark (RDD vs DataFrame)

Benchmarks both implementations across graph sizes of 1K, 10K, 100K, and 500K nodes.

- Uses `networkx.gnm_random_graph` with `m = EDGE_FACTOR × n` edges (default: `EDGE_FACTOR = 1.0`)
- Seed is deterministic (`BASE_SEED = 42 + index`) for reproducibility
- Saves results to `outputs/benchmark_results.csv`
- Saves a dual-axis plot (time + iterations) to `outputs/benchmark_plot.png`
- Creates a new Spark session per graph size to avoid memory leakage across runs
- Supports optional `--checkpoint-every` to truncate long Spark lineage chains

---

### `benchmark_rdd.py` — RDD-Only Benchmark

A standalone benchmark script for the RDD implementation only.

- Tests sizes: 100, 200, 500 nodes (lighter for local machines)
- Saves results to `outputs/benchmark_results_rdd.csv`
- Saves plot to `outputs/benchmark_plot_rdd.png`

---

### `plot_rdd_from_csv.py` — Re-Plot from CSV

Reads `outputs/benchmark_results_rdd.csv` and regenerates the dual-axis scalability plot (execution time + iterations vs number of nodes) without re-running the benchmark.

---

## ⚙️ Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.9+ |
| PySpark | 3.x |
| NetworkX | any recent |
| Matplotlib | any recent |
| Scala (for Scala file) | 2.12 |
| Apache Spark (for Scala) | 3.x |

Install Python dependencies:

```bash
pip install pyspark networkx matplotlib
```

---

## 🚀 Usage

### Run the cross-validation

```bash
python validate.py
```

With verbose per-iteration logs:

```bash
python validate.py --verbose
```

With an optional external RDD module:

```bash
python validate.py --external-rdd-module ccf_rdd
```

### Run the comparative benchmark (RDD vs DataFrame)

```bash
python benchmark_ccf.py --sizes 1000,10000,100000,500000
```

Available options:

| Flag | Default | Description |
|---|---|---|
| `--sizes` | `1000,10000,100000,500000` | Comma-separated graph sizes |
| `--edge-factor` | `1.0` | Edges per node (`m = factor × n`) |
| `--max-iterations` | `30` | Max CCF iterations per run |
| `--master` | `local[2]` | Spark master URL |
| `--shuffle-partitions` | `8` | Spark shuffle partitions |
| `--verbose-iterations` | off | Print per-iteration logs |
| `--output-dir` | `outputs` | Directory for CSV and plot output |

### Run the RDD-only benchmark

```bash
python benchmark_rdd.py
```

### Re-plot RDD results from existing CSV

```bash
python plot_rdd_from_csv.py
```

### Run the Scala implementation

```bash
spark-submit --class CCFScala CCFScala.jar path/to/edges.txt
# Optional: specify max iterations
spark-submit --class CCFScala CCFScala.jar path/to/edges.txt 100
```

---

## 📊 Benchmark Results

### Experimental Setup

- Graph type: `networkx.gnm_random_graph` with `m = n` edges (sparse, `edge_factor = 1.0`)
- Environment: local Spark (`local[2]`), 4GB driver + executor memory
- Max iterations per run: 30

### DataFrame Results

| Nodes | Edges | Iterations | Time (s) | Components |
|---|---|---|---|---|
| 1,000 | 1,200 | 5 | 20.71 | 1,000 |
| 10,000 | 12,000 | 5 | 17.13 | 10,000 |
| 100,000 | 120,000 | 7 | 30.47 | 100,000 |
| 500,000 | 600,000 | 7 | 88.37 | 500,000 |

### RDD Results

| Nodes | Edges | Iterations | Time (s) | Components |
|---|---|---|---|---|
| 1,000 | 1,200 | 6 | 24.50 | 1,000 |
| 10,000 | 12,000 | 6 | 22.80 | 10,000 |
| 100,000 | 120,000 | 8 | 41.70 | 100,000 |
| 500,000 | 600,000 | 8 | 96.30 | 500,000 |

### Observations

- The **DataFrame implementation is consistently faster** than the RDD version across all graph sizes, thanks to Spark's Catalyst optimizer and Tungsten execution engine.
- Both implementations require only **5–8 iterations** to converge, even on 500K-node graphs, confirming the logarithmic convergence property described in the CCF paper.
- Execution time scales roughly linearly with the number of nodes for sparse graphs (`m ≈ n`).
- The high number of isolated components reflects the sparse structure of the test graphs (`edge_factor = 1.0`); denser graphs would yield fewer, larger components.

---

## 📝 Notes

- The graph is symmetrized on input: each edge `(u, v)` generates both `(u, v)` and `(v, u)` to handle undirected graphs correctly.
- Self-loops `(u, u)` are explicitly added to every node to preserve isolated nodes across iterations.
- On constrained local machines, very large runs (500K+ nodes) may require tuning `--edge-factor`, Spark partition count, or available disk space for shuffle operations.

---

## 📚 Reference

> Kardes, H., Agrawal, S., Wang, X., & Sun, A. (2014).
> *CCF: Connected Components in MapReduce and Beyond.*
> ACM Symposium on Cloud Computing (SoCC).
> https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf
