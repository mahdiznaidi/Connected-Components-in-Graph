"""
Runner de benchmark pour l'implementation CCF DataFrame sur des tailles croissantes.

Exigences implementees:
- tailles de graphe: 1_000, 10_000, 100_000, 500_000 noeuds
- generation aleatoire avec NetworkX gnm_random_graph
- metriques: temps d'execution et nombre d'iterations CCF
- sortie CSV: benchmark_results.csv
- sortie graphique: benchmark_plot.png
"""

from __future__ import annotations

import argparse
import csv
import gc
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

# Doit etre defini avant l'import des bibliotheques numeriques/scientifiques.
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

import matplotlib.pyplot as plt
import networkx as nx
from pyspark.sql import SparkSession

from ccf_dataframe import run_ccf_dataframe


# Tailles de benchmark fixes demandees (par defaut).
DEFAULT_GRAPH_SIZES = [1_000, 10_000, 100_000, 500_000]

# Nombre d'arcs par graphe: m = EDGE_FACTOR * n.
# Un regime sparse garde generation et runtime Spark tractables.
EDGE_FACTOR = 1.0
MAX_ITERATIONS = 30

# Seed deterministe pour la reproductibilite.
BASE_SEED = 42

CSV_PATH = Path("benchmark_results.csv")
PLOT_PATH = Path("benchmark_plot.png")


@dataclass
class BenchmarkRow:
    num_nodes: int
    num_edges: int
    iterations: int
    elapsed_seconds: float
    components: int
    status: str
    error: str


def generate_graph_edges(num_nodes: int, edge_factor: float, seed: int) -> List[tuple[int, int]]:
    """
    Genere un graphe aleatoire non oriente avec gnm_random_graph et retourne les arcs.

    On ajoute aussi des boucles (u, u) pour tous les noeuds afin de preserver
    les noeuds isoles dans la relation d'arcs d'entree de CCF.
    """
    num_edges = max(1, int(edge_factor * num_nodes))
    g = nx.gnm_random_graph(n=num_nodes, m=num_edges, seed=seed)

    # Conversion en tuples entiers.
    edges = [(int(u), int(v)) for u, v in g.edges()]
    # Ajoute explicitement chaque noeud avec une boucle.
    edges.extend((node, node) for node in range(num_nodes))

    # Libere la structure NetworkX avant le traitement Spark.
    del g
    gc.collect()
    return edges


def write_csv(rows: Iterable[BenchmarkRow], path: Path) -> None:
    """Ecrit les lignes de benchmark dans un CSV."""
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "num_nodes",
                "num_edges",
                "iterations",
                "elapsed_seconds",
                "components",
                "status",
                "error",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.num_nodes,
                    row.num_edges,
                    row.iterations,
                    f"{row.elapsed_seconds:.6f}",
                    row.components,
                    row.status,
                    row.error,
                ]
            )


def plot_results(rows: List[BenchmarkRow], output_path: Path) -> None:
    """Trace la courbe de scalabilite: temps d'execution vs nombre de noeuds."""
    successful = [r for r in rows if r.status == "ok"]
    if not successful:
        # Cree une figure vide avec message pour eviter un echec silencieux.
        plt.figure(figsize=(9, 5))
        plt.text(0.5, 0.5, "No successful benchmark run", ha="center", va="center")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(output_path, dpi=150)
        plt.close()
        return

    x = [r.num_nodes for r in successful]
    y_time = [r.elapsed_seconds for r in successful]
    y_iter = [r.iterations for r in successful]

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(x, y_time, marker="o", linewidth=2, label="Execution time (s)", color="#005f73")
    ax1.set_xlabel("Number of nodes")
    ax1.set_ylabel("Execution time (seconds)", color="#005f73")
    ax1.tick_params(axis="y", labelcolor="#005f73")
    ax1.set_xscale("log")
    ax1.grid(True, linestyle="--", alpha=0.35)

    # Deuxieme axe pour afficher l'evolution du nombre d'iterations.
    ax2 = ax1.twinx()
    ax2.plot(x, y_iter, marker="s", linewidth=2, label="Iterations", color="#bb3e03")
    ax2.set_ylabel("Iterations", color="#bb3e03")
    ax2.tick_params(axis="y", labelcolor="#bb3e03")

    # Legende combinee.
    lines = ax1.get_lines() + ax2.get_lines()
    labels = [line.get_label() for line in lines]
    ax1.legend(lines, labels, loc="upper left")

    plt.title("CCF DataFrame Scalability Benchmark")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close(fig)


def create_spark_session(master: str, shuffle_partitions: int) -> SparkSession:
    """Cree une session Spark configuree pour le benchmark local."""
    python_exec = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)
    # Contraint les pools de threads numeriques cote workers.
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["NUMEXPR_NUM_THREADS"] = "1"

    spark = (
        SparkSession.builder.master(master)
        .appName("CCFDataFrameBenchmark")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def parse_sizes(raw: str) -> List[int]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    sizes = [int(p) for p in parts]
    if not sizes:
        raise ValueError("At least one graph size is required.")
    return sizes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Runner de benchmark CCF DataFrame.")
    parser.add_argument(
        "--sizes",
        default="1000,10000,100000,500000",
        help="Tailles de noeuds separees par des virgules. Defaut: 1000,10000,100000,500000",
    )
    parser.add_argument(
        "--edge-factor",
        type=float,
        default=EDGE_FACTOR,
        help="Facteur arcs/noeud pour gnm_random_graph (m = factor * n). Defaut: 1.0",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=MAX_ITERATIONS,
        help="Nombre maximal d'iterations CCF. Defaut: 30",
    )
    parser.add_argument(
        "--master",
        default="local[2]",
        help="Valeur du master Spark. Defaut: local[2]",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=8,
        help="Nombre de partitions de shuffle Spark. Defaut: 8",
    )
    parser.add_argument(
        "--verbose-iterations",
        action="store_true",
        help="Affiche les logs CCF a chaque iteration.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    graph_sizes = parse_sizes(args.sizes)
    rows: List[BenchmarkRow] = []

    for idx, num_nodes in enumerate(graph_sizes):
        seed = BASE_SEED + idx
        print(f"\n[Benchmark] size={num_nodes} nodes", flush=True)
        spark: Optional[SparkSession] = None
        try:
            gen_start = time.perf_counter()
            edges = generate_graph_edges(
                num_nodes=num_nodes,
                edge_factor=args.edge_factor,
                seed=seed,
            )
            num_edges = len(edges)
            gen_elapsed = time.perf_counter() - gen_start
            print(f"[Benchmark] generated edges={num_edges} in {gen_elapsed:.2f}s", flush=True)

            spark = create_spark_session(
                master=args.master,
                shuffle_partitions=args.shuffle_partitions,
            )
            edges_df = spark.createDataFrame(edges, ["src", "dst"])
            print("[Benchmark] running CCF ...", flush=True)

            start = time.perf_counter()
            components_df, history = run_ccf_dataframe(
                edges_df=edges_df,
                max_iterations=args.max_iterations,
                verbose=args.verbose_iterations,
                checkpoint_every=1,
            )
            components_count = components_df.count()
            elapsed = time.perf_counter() - start

            row = BenchmarkRow(
                num_nodes=num_nodes,
                num_edges=num_edges,
                iterations=len(history),
                elapsed_seconds=elapsed,
                components=components_count,
                status="ok",
                error="",
            )
            print(
                "[Benchmark] done "
                f"iterations={row.iterations} "
                f"time={row.elapsed_seconds:.3f}s "
                f"components={row.components}"
            )
            rows.append(row)
        except Exception as exc:  # noqa: BLE001
            row = BenchmarkRow(
                num_nodes=num_nodes,
                num_edges=0,
                iterations=0,
                elapsed_seconds=0.0,
                components=0,
                status="failed",
                error=str(exc).replace("\n", " "),
            )
            print(f"[Benchmark] failed size={num_nodes}: {row.error}")
            rows.append(row)
        finally:
            if spark is not None:
                try:
                    spark.stop()
                except Exception:
                    pass
            # Persiste la progression intermediaire meme si une taille echoue.
            write_csv(rows, CSV_PATH)
            gc.collect()

    plot_results(rows, PLOT_PATH)
    print(f"\nSaved CSV: {CSV_PATH.resolve()}")
    print(f"Saved plot: {PLOT_PATH.resolve()}")


if __name__ == "__main__":
    main()
