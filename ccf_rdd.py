from pyspark.sql import SparkSession

def make_undirected(edges):
    """
    Transforme un graphe en graphe non orienté
    """
    new_edges = []
    for u, v in edges:
        new_edges.append((u, v))
        new_edges.append((v, u))
    return new_edges


def build_adjacency(sc, edges):
    """
    Construit la liste d'adjacence initiale
    """
    edges_rdd = sc.parallelize(make_undirected(edges)).distinct()

    nodes = edges_rdd.flatMap(lambda e: [e[0], e[1]]).distinct()

    loops = nodes.map(lambda x: (x, x))

    adjacency = edges_rdd.union(loops).distinct()

    return adjacency


def ccf_iterate(adjacency):
    """
    Phase CCF-Iterate
    """

    min_neighbors = adjacency.reduceByKey(min)

    joined = adjacency.join(min_neighbors)

    emit1 = joined.map(lambda x: (x[1][1], x[1][0]))

    emit2 = joined.filter(lambda x: x[1][0] != x[1][1]) \
                  .map(lambda x: (x[1][0], x[1][1]))

    self_loops = min_neighbors.map(lambda x: (x[0], x[0]))

    return emit1.union(emit2).union(self_loops)


def ccf_dedup(old_adj, new_pairs):
    """
    Phase CCF-Dedup
    """

    new_adj = new_pairs.distinct().cache()

    added = new_adj.subtract(old_adj).count()
    removed = old_adj.subtract(new_adj).count()

    converged = (added + removed) == 0

    return new_adj, converged


def extract_components(adjacency):
    """
    Extrait les composantes connexes finales
    """

    return dict(
        adjacency.reduceByKey(min)
        .map(lambda x: (int(x[0]), int(x[1])))
        .collect()
    )


def run_ccf_rdd(spark, edges, max_iter=100, verbose=False):

    sc = spark.sparkContext

    adjacency = build_adjacency(sc, edges).cache()

    adjacency.count()

    for i in range(max_iter):

        candidates = ccf_iterate(adjacency)

        new_adj, converged = ccf_dedup(adjacency, candidates)

        if verbose:
            print("Iteration", i+1, "converged:", converged)

        adjacency.unpersist()
        adjacency = new_adj

        if converged:
            break

    return extract_components(adjacency)


def connected_components_rdd(spark, edges):
    return run_ccf_rdd(spark, edges)


def connected_components(spark, edges):
    return run_ccf_rdd(spark, edges)


def run(spark, edges):
    return run_ccf_rdd(spark, edges)