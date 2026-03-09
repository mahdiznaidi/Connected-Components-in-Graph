# Connected Components in Graph (CCF)

This repository contains our Spark implementation of the CCF algorithm
(Connected Components in Graphs via MapReduce).

Paper reference:
https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf

## My part (P2)

Implemented files:

- `ccf_dataframe.py`: full CCF implementation in PySpark DataFrame
  - CCF-Iterate
  - CCF-Dedup
  - iterative loop until convergence
  - output as `(node, component_id)`
- `CCFScala.scala`: CCF implementation in Scala Spark RDD
- `validate.py`: cross-validation script (DataFrame vs RDD reference, with optional external RDD module)
- `benchmark_ccf.py`: scalability benchmark runner using `networkx.gnm_random_graph`
- `benchmark_results.csv`: benchmark output (execution time + iterations)
- `benchmark_plot.png`: scalability curve

## Requirements

- Python 3.9+ (or compatible)
- PySpark 3.x
- Scala 2.12 + Spark 3.x (for Scala execution)
- Python packages:
  - `pyspark`
  - `networkx`
  - `matplotlib`

## Run validation

```bash
python validate.py
```

Optional external RDD module:

```bash
python validate.py --external-rdd-module your_module_name
```

## Run benchmark

Default requested sizes:

```bash
python benchmark_ccf.py --sizes 1000,10000,100000,500000
```

Outputs:

- `benchmark_results.csv`
- `benchmark_plot.png`

## Notes

- Graph generation for benchmarks uses `networkx.gnm_random_graph`.
- In constrained local environments, very large runs may require tuning
  (`--edge-factor`, Spark partitions, and available disk space).
