<<<<<<< HEAD
=======
"""
Runner de benchmark pour l'implementation CCF DataFrame sur des tailles croissantes.

Exigences implementees:
- tailles de graphe: 1_000, 10_000, 100_000, 500_000 noeuds
- generation aleatoire avec NetworkX gnm_random_graph
- metriques: temps d'execution et nombre d'iterations CCF
- sortie CSV: outputs/benchmark_results.csv
- sortie graphique: outputs/benchmark_plot.png
"""

>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
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

<<<<<<< HEAD
=======
# Doit etre defini avant l'import des bibliotheques numeriques/scientifiques.
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

import matplotlib.pyplot as plt
import networkx as nx
from pyspark.sql import SparkSession

from ccf_dataframe import run_ccf_dataframe
<<<<<<< HEAD
from ccf_rdd import run_ccf_rdd

DEFAULT_GRAPH_SIZES = [1_000, 10_000, 100_000, 500_000]
EDGE_FACTOR = 1.0
MAX_ITERATIONS = 30
BASE_SEED = 42
DEFAULT_OUTPUT_DIR = Path("outputs")
CSV_FILENAME = "benchmark_comparison_results.csv"
PLOT_FILENAME = "benchmark_comparison_plot.png"
=======


# Tailles de benchmark fixes demandees (par defaut).
DEFAULT_GRAPH_SIZES = [1_000, 10_000, 100_000, 500_000]

# Nombre d'arcs par graphe: m = EDGE_FACTOR * n.
# Un regime sparse garde generation et runtime Spark tractables.
EDGE_FACTOR = 1.0
MAX_ITERATIONS = 30

# Seed deterministe pour la reproductibilite.
BASE_SEED = 42

DEFAULT_OUTPUT_DIR = Path("outputs")
CSV_FILENAME = "benchmark_results.csv"
PLOT_FILENAME = "benchmark_plot.png"
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5


@dataclass
class BenchmarkRow:
    num_nodes: int
    num_edges: int
<<<<<<< HEAD
    dataframe_iterations: int
    dataframe_elapsed_seconds: float
    dataframe_components: int
    dataframe_status: str
    dataframe_error: str
    rdd_elapsed_seconds: float
    rdd_components: int
    rdd_status: str
    rdd_error: str


def generate_graph_edges(num_nodes: int, edge_factor: float, seed: int) -> List[tuple[int, int]]:
    num_edges = max(1, int(edge_factor * num_nodes))
    g = nx.gnm_random_graph(n=num_nodes, m=num_edges, seed=seed)

    edges = [(int(u), int(v)) for u, v in g.edges()]
    edges.extend((node, node) for node in range(num_nodes))

=======
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
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
    del g
    gc.collect()
    return edges


def write_csv(rows: Iterable[BenchmarkRow], path: Path) -> None:
<<<<<<< HEAD
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "num_nodes",
            "num_edges",
            "dataframe_iterations",
            "dataframe_elapsed_seconds",
            "dataframe_components",
            "dataframe_status",
            "dataframe_error",
            "rdd_elapsed_seconds",
            "rdd_components",
            "rdd_status",
            "rdd_error",
        ])

        for row in rows:
            writer.writerow([
                row.num_nodes,
                row.num_edges,
                row.dataframe_iterations,
                f"{row.dataframe_elapsed_seconds:.6f}",
                row.dataframe_components,
                row.dataframe_status,
                row.dataframe_error,
                f"{row.rdd_elapsed_seconds:.6f}",
                row.rdd_components,
                row.rdd_status,
                row.rdd_error,
            ])


def plot_results(rows: List[BenchmarkRow], output_path: Path) -> None:
    ok_df = [r for r in rows if r.dataframe_status == "ok"]
    ok_rdd = [r for r in rows if r.rdd_status == "ok"]

    plt.figure(figsize=(10, 6))

    if ok_df:
        x_df = [r.num_nodes for r in ok_df]
        y_df = [r.dataframe_elapsed_seconds for r in ok_df]
        plt.plot(x_df, y_df, marker="o", linewidth=2, label="DataFrame")

    if ok_rdd:
        x_rdd = [r.num_nodes for r in ok_rdd]
        y_rdd = [r.rdd_elapsed_seconds for r in ok_rdd]
        plt.plot(x_rdd, y_rdd, marker="s", linewidth=2, label="RDD")

    plt.xlabel("Number of nodes")
    plt.ylabel("Execution time (seconds)")
    plt.xscale("log")
    plt.grid(True, linestyle="--", alpha=0.35)
    plt.title("CCF Scalability Benchmark: RDD vs DataFrame")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def create_spark_session(master: str, shuffle_partitions: int) -> SparkSession:
    python_exec = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)

=======
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
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["NUMEXPR_NUM_THREADS"] = "1"

    spark = (
        SparkSession.builder.master(master)
<<<<<<< HEAD
        .appName("CCFBenchmarkComparison")
=======
        .appName("CCFDataFrameBenchmark")
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
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
<<<<<<< HEAD
    parser = argparse.ArgumentParser(description="Runner de benchmark CCF RDD vs DataFrame.")
    default_sizes = ",".join(str(s) for s in DEFAULT_GRAPH_SIZES)

=======
    parser = argparse.ArgumentParser(description="Runner de benchmark CCF DataFrame.")
    default_sizes = ",".join(str(s) for s in DEFAULT_GRAPH_SIZES)
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
    parser.add_argument(
        "--sizes",
        default=default_sizes,
        help=f"Tailles de noeuds separees par des virgules. Defaut: {default_sizes}",
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
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="Dossier de sortie pour le CSV et le graphique. Defaut: outputs",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    graph_sizes = parse_sizes(args.sizes)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
<<<<<<< HEAD

    csv_path = output_dir / CSV_FILENAME
    plot_path = output_dir / PLOT_FILENAME

=======
    csv_path = output_dir / CSV_FILENAME
    plot_path = output_dir / PLOT_FILENAME
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
    rows: List[BenchmarkRow] = []

    for idx, num_nodes in enumerate(graph_sizes):
        seed = BASE_SEED + idx
        print(f"\n[Benchmark] size={num_nodes} nodes", flush=True)
<<<<<<< HEAD

        spark: Optional[SparkSession] = None

        dataframe_iterations = 0
        dataframe_elapsed = 0.0
        dataframe_components = 0
        dataframe_status = "failed"
        dataframe_error = ""

        rdd_elapsed = 0.0
        rdd_components = 0
        rdd_status = "failed"
        rdd_error = ""

=======
        spark: Optional[SparkSession] = None
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
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
<<<<<<< HEAD

            # DataFrame benchmark
            try:
                edges_df = spark.createDataFrame(edges, ["src", "dst"])
                print("[Benchmark] running DataFrame ...", flush=True)

                start_df = time.perf_counter()
                components_df, history = run_ccf_dataframe(
                    edges_df=edges_df,
                    max_iterations=args.max_iterations,
                    verbose=args.verbose_iterations,
                    checkpoint_every=1,
                )
                dataframe_components = components_df.count()
                dataframe_elapsed = time.perf_counter() - start_df
                dataframe_iterations = len(history)
                dataframe_status = "ok"
            except Exception as exc:
                dataframe_error = str(exc).replace("\n", " ")

            # RDD benchmark
            try:
                print("[Benchmark] running RDD ...", flush=True)

                start_rdd = time.perf_counter()
                components_rdd = run_ccf_rdd(
                    spark=spark,
                    edges=edges,
                    max_iterations=args.max_iterations,
                    verbose=args.verbose_iterations,
                )
                rdd_elapsed = time.perf_counter() - start_rdd
                rdd_components = len(set(components_rdd.values()))
                rdd_status = "ok"
            except Exception as exc:
                rdd_error = str(exc).replace("\n", " ")
=======
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
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5

            row = BenchmarkRow(
                num_nodes=num_nodes,
                num_edges=num_edges,
<<<<<<< HEAD
                dataframe_iterations=dataframe_iterations,
                dataframe_elapsed_seconds=dataframe_elapsed,
                dataframe_components=dataframe_components,
                dataframe_status=dataframe_status,
                dataframe_error=dataframe_error,
                rdd_elapsed_seconds=rdd_elapsed,
                rdd_components=rdd_components,
                rdd_status=rdd_status,
                rdd_error=rdd_error,
            )
            rows.append(row)

            print(
                "[Benchmark] done "
                f"DF=({dataframe_status}, {dataframe_elapsed:.3f}s) "
                f"RDD=({rdd_status}, {rdd_elapsed:.3f}s)"
            )

        except Exception as exc:
            row = BenchmarkRow(
                num_nodes=num_nodes,
                num_edges=0,
                dataframe_iterations=0,
                dataframe_elapsed_seconds=0.0,
                dataframe_components=0,
                dataframe_status="failed",
                dataframe_error=str(exc).replace("\n", " "),
                rdd_elapsed_seconds=0.0,
                rdd_components=0,
                rdd_status="failed",
                rdd_error=str(exc).replace("\n", " "),
            )
            rows.append(row)
            print(f"[Benchmark] failed size={num_nodes}: {row.dataframe_error}")

=======
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
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
        finally:
            if spark is not None:
                try:
                    spark.stop()
                except Exception:
                    pass
<<<<<<< HEAD

=======
            # Persiste la progression intermediaire meme si une taille echoue.
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
            write_csv(rows, csv_path)
            gc.collect()

    plot_results(rows, plot_path)
<<<<<<< HEAD

=======
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
    print(f"\nSaved CSV: {csv_path.resolve()}")
    print(f"Saved plot: {plot_path.resolve()}")


if __name__ == "__main__":
<<<<<<< HEAD
    main()
=======
    main()
>>>>>>> 5a0b3ef09c03a0c48944f298b4fb418070e738c5
