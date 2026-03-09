"""
CCF (Connected Components via MapReduce) implemented with PySpark DataFrames.

This module follows the two phases described in the paper:
1) CCF-Iterate
2) CCF-Dedup

Input graph format:
    DataFrame[src, dst]

Output format:
    DataFrame[node, component_id]
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class IterationStats:
    """Bookkeeping information for one CCF iteration."""

    iteration: int
    added_edges: int
    removed_edges: int
    converged: bool


def _require_edge_columns(edges_df: DataFrame) -> None:
    """Validate that the input DataFrame has the expected edge schema."""
    required = {"src", "dst"}
    missing = required.difference(edges_df.columns)
    if missing:
        raise ValueError(
            f"Input DataFrame must contain columns {sorted(required)}. Missing: {sorted(missing)}"
        )


def normalize_edges(edges_df: DataFrame) -> DataFrame:
    """
    Normalize input edges into an undirected relation.

    Why:
    - Connected components are defined on an undirected graph.
    - We remove duplicate edges to keep each iteration deterministic and stable.
    """
    _require_edge_columns(edges_df)

    directed = edges_df.select(
        F.col("src").cast("long").alias("src"),
        F.col("dst").cast("long").alias("dst"),
    )
    reversed_edges = directed.select(F.col("dst").alias("src"), F.col("src").alias("dst"))
    return directed.unionByName(reversed_edges).dropDuplicates(["src", "dst"])


def build_initial_adjacency(edges_df: DataFrame) -> DataFrame:
    """
    Build initial adjacency pairs (node, neighbor) with explicit self-loops.

    The CCF paper emits (l, l) during iterate. Keeping self-loops directly in the
    adjacency relation makes this behavior explicit and guarantees every node
    remains present across iterations.
    """
    undirected = normalize_edges(edges_df)
    adjacency = undirected.select(F.col("src").alias("node"), F.col("dst").alias("neighbor"))

    nodes = adjacency.select("node").union(adjacency.select(F.col("neighbor").alias("node"))).distinct()
    self_loops = nodes.select("node", F.col("node").alias("neighbor"))
    return adjacency.unionByName(self_loops).dropDuplicates(["node", "neighbor"])


def ccf_iterate(adjacency_df: DataFrame) -> DataFrame:
    """
    CCF-Iterate phase.

    For each node l and its neighborhood N_l:
    - compute m = min(N_l)
    - emit (m, n) for every n in N_l
    - emit (n, m) for every n in N_l where n != m
    - keep (l, l) self-loop

    Required DataFrame operators used here:
    - groupBy + agg(min)
    - join
    - withColumn
    """
    # 1) Compute m = min(neighbors) per node.
    min_neighbors = adjacency_df.groupBy("node").agg(F.min("neighbor").alias("min_neighbor"))

    # 2) Re-attach each node's minimum to every (node, neighbor) pair.
    joined = adjacency_df.join(min_neighbors, on="node", how="inner").withColumn(
        "is_min_neighbor", F.col("neighbor") == F.col("min_neighbor")
    )

    # 3) Emit (m, n): pull all neighbors under the minimum label.
    emit_to_min = joined.select(
        F.col("min_neighbor").alias("node"),
        F.col("neighbor").alias("neighbor"),
    )

    # 4) Emit (n, m) only when n != m (same condition as in the paper).
    emit_reverse = joined.filter(~F.col("is_min_neighbor")).select(
        F.col("neighbor").alias("node"),
        F.col("min_neighbor").alias("neighbor"),
    )

    # 5) Explicitly keep self-loops so every node survives the next iteration.
    self_loops = min_neighbors.select(F.col("node"), F.col("node").alias("neighbor"))

    return emit_to_min.unionByName(emit_reverse).unionByName(self_loops)


def ccf_dedup(previous_adjacency: DataFrame, candidates: DataFrame) -> Tuple[DataFrame, int, int, bool]:
    """
    CCF-Dedup phase.

    Steps:
    - remove duplicate pairs
    - detect convergence by checking if edge-set changed compared to previous iteration
    """
    next_adjacency = candidates.dropDuplicates(["node", "neighbor"])

    # Added edges: present in next, absent in previous.
    added_edges = next_adjacency.join(previous_adjacency, on=["node", "neighbor"], how="left_anti").count()

    # Removed edges: present in previous, absent in next.
    removed_edges = previous_adjacency.join(next_adjacency, on=["node", "neighbor"], how="left_anti").count()

    converged = (added_edges + removed_edges) == 0
    return next_adjacency, added_edges, removed_edges, converged


def run_ccf_dataframe(
    edges_df: DataFrame,
    max_iterations: int = 100,
    verbose: bool = True,
    checkpoint_every: int | None = None,
) -> Tuple[DataFrame, list[IterationStats]]:
    """
    Execute CCF until stabilization.

    Returns:
    - components DataFrame[node, component_id]
    - per-iteration statistics

    Optional performance control:
    - checkpoint_every: if set (e.g. 1), periodically truncates Spark lineage
      to avoid very large logical plans on long iterative runs.
    """
    adjacency = build_initial_adjacency(edges_df).cache()
    adjacency.count()  # materialize cache

    history: list[IterationStats] = []

    for iteration in range(1, max_iterations + 1):
        candidates = ccf_iterate(adjacency)
        next_adjacency, added, removed, converged = ccf_dedup(adjacency, candidates)

        # Optional lineage truncation for long iterative workloads (benchmarks).
        if checkpoint_every is not None and checkpoint_every > 0 and (iteration % checkpoint_every == 0):
            next_adjacency = next_adjacency.localCheckpoint(eager=False)

        next_adjacency = next_adjacency.cache()
        next_adjacency.count()  # materialize before releasing previous cache
        adjacency.unpersist()

        stats = IterationStats(
            iteration=iteration,
            added_edges=added,
            removed_edges=removed,
            converged=converged,
        )
        history.append(stats)

        if verbose:
            print(
                f"[CCF-DataFrame] iteration={iteration} "
                f"added={added} removed={removed} converged={converged}"
            )

        adjacency = next_adjacency
        if converged:
            break

    components = (
        adjacency.groupBy("node")
        .agg(F.min("neighbor").alias("component_id"))
        .select("node", "component_id")
        .orderBy("node")
    )

    return components, history


def _demo() -> None:
    """Small local demo runnable with `python ccf_dataframe.py`."""
    # On Windows lab setups, Spark often defaults to `python3` even when only
    # `python` exists. We force worker/driver executables to the current one.
    python_exec = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("CCFDataFrameDemo")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Three components: {1,2,3}, {4,5}, {6}
    demo_edges = [(1, 2), (2, 3), (4, 5), (6, 6)]
    edges_df = spark.createDataFrame(demo_edges, ["src", "dst"])

    components_df, history = run_ccf_dataframe(edges_df, verbose=True)
    components_df.show(truncate=False)
    print(f"Iterations executed: {len(history)}")

    spark.stop()


if __name__ == "__main__":
    _demo()
