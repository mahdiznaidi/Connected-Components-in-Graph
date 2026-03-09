from __future__ import annotations

import csv
import gc
import os
import sys
import time
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx
from pyspark.sql import SparkSession

from ccf_rdd import run_ccf_rdd

# Réglages du benchmark RDD
GRAPH_SIZES = [100, 200, 500]
EDGE_FACTOR = 1.0
MAX_ITERATIONS = 30
BASE_SEED = 42
OUTPUT_DIR = Path("outputs")
CSV_FILE = OUTPUT_DIR / "benchmark_results_rdd.csv"
PLOT_FILE = OUTPUT_DIR / "benchmark_plot_rdd.png"


def generate_graph_edges(num_nodes: int, edge_factor: float, seed: int):
    """
    Génère un graphe aléatoire sous forme de liste d'arêtes.
    On ajoute aussi des self-loops pour stabiliser l'adjacence.
    """
    num_edges = max(1, int(edge_factor * num_nodes))
    g = nx.gnm_random_graph(n=num_nodes, m=num_edges, seed=seed)

    edges = [(int(u), int(v)) for u, v in g.edges()]
    edges.extend((node, node) for node in range(num_nodes))

    del g
    gc.collect()
    return edges


def create_spark_session():
    """
    Crée une session Spark simple et légère pour Windows / VS Code.
    """
    python_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("CCF_RDD_Benchmark")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def run_benchmark():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results = []

    for i, num_nodes in enumerate(GRAPH_SIZES):
        seed = BASE_SEED + i
        expected_num_edges = max(1, int(EDGE_FACTOR * num_nodes))

        print(f"\n[Benchmark RDD] size={num_nodes} nodes")

        spark = None
        elapsed = 0.0
        num_components = 0
        status = "failed"
        error = ""

        try:
            edges = generate_graph_edges(num_nodes, EDGE_FACTOR, seed)
            print(f"[Benchmark RDD] generated {len(edges)} edges")

            spark = create_spark_session()

            start = time.perf_counter()

            components = run_ccf_rdd(
                spark=spark,
                edges=edges,
                max_iter=MAX_ITERATIONS,
                verbose=False,
            )

            elapsed = time.perf_counter() - start
            num_components = len(set(components.values()))
            status = "ok"

            print(f"[Benchmark RDD] done in {elapsed:.3f}s")

        except Exception as exc:
            error = str(exc).replace("\n", " ")
            print(f"[Benchmark RDD] failed: {error}")

        finally:
            if spark is not None:
                try:
                    spark.stop()
                except Exception:
                    pass

            results.append({
                "num_nodes": num_nodes,
                "num_edges": expected_num_edges,
                "elapsed_seconds": elapsed,
                "components": num_components,
                "status": status,
                "error": error,
            })

            gc.collect()

    return results


def save_csv(results):
    with CSV_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "num_nodes",
            "num_edges",
            "elapsed_seconds",
            "components",
            "status",
            "error",
        ])

        for row in results:
            writer.writerow([
                row["num_nodes"],
                row["num_edges"],
                f'{row["elapsed_seconds"]:.6f}',
                row["components"],
                row["status"],
                row["error"],
            ])


def plot_results(results):
    ok_rows = [r for r in results if r["status"] == "ok"]

    if not ok_rows:
        print("Aucun résultat valide à tracer.")
        return

    x = [r["num_nodes"] for r in ok_rows]
    y = [r["elapsed_seconds"] for r in ok_rows]

    plt.figure(figsize=(10, 6))
    plt.plot(x, y, marker="o", linewidth=2)
    plt.xlabel("Number of nodes")
    plt.ylabel("Execution time (seconds)")
    plt.xscale("log")
    plt.grid(True, linestyle="--", alpha=0.35)
    plt.title("CCF RDD Scalability Benchmark")
    plt.tight_layout()
    plt.savefig(PLOT_FILE, dpi=150)
    plt.close()


def main():
    results = run_benchmark()
    save_csv(results)
    plot_results(results)

    print(f"\nSaved CSV: {CSV_FILE.resolve()}")
    print(f"Saved plot: {PLOT_FILE.resolve()}")


if __name__ == "__main__":
    main()