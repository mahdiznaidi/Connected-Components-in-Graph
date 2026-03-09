"""
CCF (Connected Components via MapReduce) implemente avec des DataFrames PySpark.

Ce module suit les deux phases decrites dans le papier:
1) CCF-Iterate
2) CCF-Dedup

Format du graphe en entree:
    DataFrame[src, dst]

Format de sortie:
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
    """Informations de suivi pour une iteration CCF."""

    iteration: int
    added_edges: int
    removed_edges: int
    converged: bool


def _require_edge_columns(edges_df: DataFrame) -> None:
    """Valide que le DataFrame en entree possede le schema d'arcs attendu."""
    required = {"src", "dst"}
    missing = required.difference(edges_df.columns)
    if missing:
        raise ValueError(
            f"Input DataFrame must contain columns {sorted(required)}. Missing: {sorted(missing)}"
        )


def normalize_edges(edges_df: DataFrame) -> DataFrame:
    """
    Normalise les arcs d'entree en une relation non orientee.

    Pourquoi:
    - Les composantes connexes sont definies sur un graphe non oriente.
    - On retire les doublons pour garder des iterations deterministes et stables.
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
    Construit les couples d'adjacence initiaux (node, neighbor) avec boucles explicites.

    Le papier CCF emet (l, l) pendant l'iteration. Garder les boucles dans
    la relation d'adjacence rend ce comportement explicite et garantit que
    chaque noeud reste present au fil des iterations.
    """
    undirected = normalize_edges(edges_df)
    adjacency = undirected.select(F.col("src").alias("node"), F.col("dst").alias("neighbor"))

    nodes = adjacency.select("node").union(adjacency.select(F.col("neighbor").alias("node"))).distinct()
    self_loops = nodes.select("node", F.col("node").alias("neighbor"))
    return adjacency.unionByName(self_loops).dropDuplicates(["node", "neighbor"])


def ccf_iterate(adjacency_df: DataFrame) -> DataFrame:
    """
    Phase CCF-Iterate.

    Pour chaque noeud l et son voisinage N_l:
    - calcule m = min(N_l)
    - emet (m, n) pour chaque n dans N_l
    - emet (n, m) pour chaque n dans N_l avec n != m
    - conserve la boucle (l, l)

    Operateurs DataFrame requis ici:
    - groupBy + agg(min)
    - join
    - withColumn
    """
    # 1) Calcule m = min(neighbors) pour chaque noeud.
    min_neighbors = adjacency_df.groupBy("node").agg(F.min("neighbor").alias("min_neighbor"))

    # 2) Rattache le minimum de chaque noeud a chaque couple (node, neighbor).
    joined = adjacency_df.join(min_neighbors, on="node", how="inner").withColumn(
        "is_min_neighbor", F.col("neighbor") == F.col("min_neighbor")
    )

    # 3) Emet (m, n): rattache tous les voisins a l'etiquette minimale.
    emit_to_min = joined.select(
        F.col("min_neighbor").alias("node"),
        F.col("neighbor").alias("neighbor"),
    )

    # 4) Emet (n, m) seulement si n != m (meme condition que dans le papier).
    emit_reverse = joined.filter(~F.col("is_min_neighbor")).select(
        F.col("neighbor").alias("node"),
        F.col("min_neighbor").alias("neighbor"),
    )

    # 5) Conserve explicitement les boucles pour garder tous les noeuds.
    self_loops = min_neighbors.select(F.col("node"), F.col("node").alias("neighbor"))

    return emit_to_min.unionByName(emit_reverse).unionByName(self_loops)


def ccf_dedup(previous_adjacency: DataFrame, candidates: DataFrame) -> Tuple[DataFrame, int, int, bool]:
    """
    Phase CCF-Dedup.

    Etapes:
    - retire les couples dupliques
    - detecte la convergence en comparant l'ensemble d'arcs a l'iteration precedente
    """
    next_adjacency = candidates.dropDuplicates(["node", "neighbor"])

    # Arcs ajoutes: presents dans next, absents dans previous.
    added_edges = next_adjacency.join(previous_adjacency, on=["node", "neighbor"], how="left_anti").count()

    # Arcs retires: presents dans previous, absents dans next.
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
    Execute CCF jusqu'a stabilisation.

    Retourne:
    - un DataFrame components[node, component_id]
    - les statistiques par iteration

    Controle de performance optionnel:
    - checkpoint_every: si defini (ex: 1), tronque periodiquement la lineage Spark
      pour eviter des plans logiques trop volumineux sur des executions longues.
    """
    adjacency = build_initial_adjacency(edges_df).cache()
    adjacency.count()  # materialise le cache

    history: list[IterationStats] = []

    for iteration in range(1, max_iterations + 1):
        candidates = ccf_iterate(adjacency)
        next_adjacency, added, removed, converged = ccf_dedup(adjacency, candidates)

        # Troncature optionnelle de lineage pour les charges iteratives longues.
        if checkpoint_every is not None and checkpoint_every > 0 and (iteration % checkpoint_every == 0):
            next_adjacency = next_adjacency.localCheckpoint(eager=False)

        next_adjacency = next_adjacency.cache()
        next_adjacency.count()  # materialise avant de liberer l'ancien cache
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
    """Petite demo locale executable avec `python ccf_dataframe.py`."""
    # Sur certains postes Windows, Spark prend `python3` par defaut meme
    # si seul `python` existe. On force les executables worker/driver.
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

    # Trois composantes: {1,2,3}, {4,5}, {6}
    demo_edges = [(1, 2), (2, 3), (4, 5), (6, 6)]
    edges_df = spark.createDataFrame(demo_edges, ["src", "dst"])

    components_df, history = run_ccf_dataframe(edges_df, verbose=True)
    components_df.show(truncate=False)
    print(f"Iterations executed: {len(history)}")

    spark.stop()


if __name__ == "__main__":
    _demo()
