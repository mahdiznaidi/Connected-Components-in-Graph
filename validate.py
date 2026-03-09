"""
Cross-validation script for CCF implementations.

What this script checks:
1) PySpark DataFrame CCF result
2) Python RDD reference result (same algorithmic logic)
3) Optional external RDD implementation (if module provided)
4) Expected components on a known toy graph
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import SparkSession

from ccf_dataframe import run_ccf_dataframe


Edge = Tuple[int, int]
ComponentMap = Dict[int, int]
ExternalRunner = Callable[..., object]


@dataclass
class ValidationReport:
    dataframe_ok: bool
    rdd_reference_ok: bool
    dataframe_vs_rdd_ok: bool
    external_rdd_checked: bool
    external_rdd_ok: bool


def build_test_graph() -> Tuple[List[Edge], ComponentMap]:
    """
    Build a deterministic graph with 10 nodes and known connected components.

    Components:
    - {1,2,3}  -> component id 1
    - {4,5}    -> component id 4
    - {6,7,8}  -> component id 6
    - {9}      -> component id 9
    - {10}     -> component id 10
    """
    edges: List[Edge] = [
        (1, 2),
        (2, 3),
        (4, 5),
        (6, 7),
        (7, 8),
        (8, 6),
        # self-loops to keep isolated nodes explicit in edge-list representation
        (9, 9),
        (10, 10),
        # duplicates / reverse duplicates to test dedup robustness
        (2, 1),
        (5, 4),
    ]

    expected: ComponentMap = {
        1: 1,
        2: 1,
        3: 1,
        4: 4,
        5: 4,
        6: 6,
        7: 6,
        8: 6,
        9: 9,
        10: 10,
    }
    return edges, expected


def run_ccf_rdd_reference(
    spark: SparkSession,
    edges: Sequence[Edge],
    max_iterations: int = 100,
    verbose: bool = False,
) -> ComponentMap:
    """
    Spark RDD reference implementation with the same CCF logic.

    Implementation note:
    For tiny validation graphs, we intentionally use 1 partition and compare
    edge sets on the driver to keep runtime predictable on constrained laptops.
    """
    sc = spark.sparkContext
    raw_edges = sc.parallelize(list(edges), 1)

    # Undirected + deduplicated relation.
    undirected = raw_edges.flatMap(
        lambda e: [e] if e[0] == e[1] else [e, (e[1], e[0])]
    ).distinct(numPartitions=1)

    # Build adjacency with self-loops.
    nodes = undirected.flatMap(lambda e: [e[0], e[1]]).distinct(numPartitions=1)
    adjacency = undirected.union(nodes.map(lambda n: (n, n))).distinct(numPartitions=1)
    current_edges = set((int(a), int(b)) for a, b in adjacency.collect())

    for iteration in range(1, max_iterations + 1):
        adjacency_rdd = sc.parallelize(list(current_edges), 1)
        min_neighbor = adjacency_rdd.reduceByKey(min, numPartitions=1)
        joined = adjacency_rdd.join(min_neighbor, numPartitions=1)

        emit_to_min = joined.map(lambda kv: (kv[1][1], kv[1][0]))
        emit_reverse = joined.filter(lambda kv: kv[1][0] != kv[1][1]).map(
            lambda kv: (kv[1][0], kv[1][1])
        )
        self_loops = min_neighbor.map(lambda kv: (kv[0], kv[0]))

        candidates = emit_to_min.union(emit_reverse).union(self_loops).distinct(numPartitions=1)
        next_edges = set((int(a), int(b)) for a, b in candidates.collect())

        added = len(next_edges.difference(current_edges))
        removed = len(current_edges.difference(next_edges))
        converged = next_edges == current_edges

        current_edges = next_edges

        if verbose:
            print(
                f"[CCF-RDD-Reference] iteration={iteration} "
                f"added={added} removed={removed} converged={converged}"
            )
        if converged:
            break

    result: Dict[int, int] = {}
    for node, neighbor in current_edges:
        if node not in result or neighbor < result[node]:
            result[node] = neighbor
    return result


def load_external_rdd_runner(module_name: Optional[str]) -> Optional[ExternalRunner]:
    """
    Optional import helper for teammate RDD implementation.
    Expected callable names (first found is used):
    - run_ccf_rdd
    - connected_components_rdd
    - connected_components
    - run
    """
    if not module_name:
        return None

    mod = importlib.import_module(module_name)
    for fn_name in ("run_ccf_rdd", "connected_components_rdd", "connected_components", "run"):
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            return fn
    raise AttributeError(
        f"Module '{module_name}' loaded but no supported runner function was found."
    )


def normalize_external_result(raw_result: object) -> ComponentMap:
    """
    Normalize multiple possible return types from an external runner:
    - dict[node -> component_id]
    - list/tuple of (node, component_id)
    """
    if isinstance(raw_result, dict):
        return {int(k): int(v) for k, v in raw_result.items()}

    if isinstance(raw_result, (list, tuple)):
        return {int(node): int(comp) for node, comp in raw_result}

    raise TypeError(
        "Unsupported external runner result type. Use dict or list/tuple[(node, component_id)]."
    )


def diff_components(left: ComponentMap, right: ComponentMap) -> List[str]:
    """Return human-readable diff lines for component maps."""
    lines: List[str] = []
    all_nodes = sorted(set(left.keys()).union(right.keys()))
    for node in all_nodes:
        lv = left.get(node)
        rv = right.get(node)
        if lv != rv:
            lines.append(f"node={node}: left={lv} right={rv}")
    return lines


def print_component_map(title: str, mapping: ComponentMap) -> None:
    """Print a compact sorted component mapping."""
    print(title)
    for node in sorted(mapping.keys()):
        print(f"  node={node:>2} -> component_id={mapping[node]}")


def run_validation(external_module: Optional[str], verbose: bool) -> ValidationReport:
    # Spark workers must use a valid Python executable on this machine.
    python_exec = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("CCFValidation")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        edges, expected = build_test_graph()
        edges_df = spark.createDataFrame(edges, ["src", "dst"])

        # 1) DataFrame implementation under test.
        dataframe_components_df, dataframe_history = run_ccf_dataframe(
            edges_df, max_iterations=20, verbose=verbose
        )
        dataframe_map: ComponentMap = {
            int(row["node"]): int(row["component_id"])
            for row in dataframe_components_df.collect()
        }

        # 2) Internal RDD reference implementation.
        rdd_reference_map = run_ccf_rdd_reference(
            spark, edges, max_iterations=20, verbose=verbose
        )

        # 3) Optional external RDD module from teammate.
        external_checked = False
        external_ok = True
        external_map: Optional[ComponentMap] = None
        external_runner = load_external_rdd_runner(external_module)
        if external_runner is not None:
            external_checked = True
            raw_external = external_runner(spark, edges)
            external_map = normalize_external_result(raw_external)
            external_ok = external_map == dataframe_map

        # 4) Compute comparisons.
        dataframe_ok = dataframe_map == expected
        rdd_reference_ok = rdd_reference_map == expected
        dataframe_vs_rdd_ok = dataframe_map == rdd_reference_map

        # 5) Clear report.
        print("=== CCF Cross Validation Report ===")
        print(f"DataFrame iterations executed : {len(dataframe_history)}")
        print(f"DataFrame vs expected         : {'OK' if dataframe_ok else 'FAIL'}")
        print(f"RDD reference vs expected     : {'OK' if rdd_reference_ok else 'FAIL'}")
        print(f"DataFrame vs RDD reference    : {'OK' if dataframe_vs_rdd_ok else 'FAIL'}")
        if external_checked:
            print(f"External RDD vs DataFrame     : {'OK' if external_ok else 'FAIL'}")
        else:
            print("External RDD vs DataFrame     : SKIPPED (no module provided)")
        print()

        print_component_map("Expected components:", expected)
        print()
        print_component_map("DataFrame components:", dataframe_map)
        print()
        print_component_map("RDD reference components:", rdd_reference_map)
        print()

        if not dataframe_ok:
            print("Diff (DataFrame vs Expected):")
            for line in diff_components(dataframe_map, expected):
                print(f"  {line}")
            print()

        if not dataframe_vs_rdd_ok:
            print("Diff (DataFrame vs RDD reference):")
            for line in diff_components(dataframe_map, rdd_reference_map):
                print(f"  {line}")
            print()

        if external_checked and not external_ok and external_map is not None:
            print("Diff (DataFrame vs External RDD):")
            for line in diff_components(dataframe_map, external_map):
                print(f"  {line}")
            print()

        return ValidationReport(
            dataframe_ok=dataframe_ok,
            rdd_reference_ok=rdd_reference_ok,
            dataframe_vs_rdd_ok=dataframe_vs_rdd_ok,
            external_rdd_checked=external_checked,
            external_rdd_ok=external_ok,
        )
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CCF DataFrame/RDD cross-validation script.")
    parser.add_argument(
        "--external-rdd-module",
        type=str,
        default=None,
        help=(
            "Optional Python module name that exposes an RDD runner "
            "(run_ccf_rdd / connected_components_rdd / connected_components / run)."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-iteration logs for DataFrame and RDD reference runs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = run_validation(args.external_rdd_module, args.verbose)

    failed = (
        (not report.dataframe_ok)
        or (not report.rdd_reference_ok)
        or (not report.dataframe_vs_rdd_ok)
        or (report.external_rdd_checked and not report.external_rdd_ok)
    )
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
