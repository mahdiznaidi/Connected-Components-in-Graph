import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * CCF (Connected Components via MapReduce) implemente avec Spark RDD en Scala.
 *
 * Entree:
 *   RDD[(src, dst)] ou chaque tuple represente un arc.
 *
 * Sortie:
 *   RDD[(node, componentId)] ou componentId est l'id minimal de la composante.
 */
object CCFScala {

  type Edge = (Long, Long)

  /**
   * Convertit les arcs en relation non orientee et retire les doublons.
   * Cela reproduit la normalisation de graphe de la version PySpark DataFrame.
   */
  def normalizeEdges(edges: RDD[Edge]): RDD[Edge] = {
    val directed = edges.flatMap {
      case (src, dst) if src == dst => Seq((src, dst))
      case (src, dst)               => Seq((src, dst), (dst, src))
    }
    directed.distinct()
  }

  /**
   * Construit la relation d'adjacence (node, neighbor) avec boucles explicites.
   * Les boucles garantissent la presence de chaque noeud a chaque iteration.
   */
  def buildInitialAdjacency(edges: RDD[Edge]): RDD[Edge] = {
    val undirected = normalizeEdges(edges)
    val nodes = undirected.flatMap { case (src, dst) => Seq(src, dst) }.distinct()
    val selfLoops = nodes.map(node => (node, node))
    undirected.union(selfLoops).distinct()
  }

  /**
   * Phase CCF-Iterate.
   *
   * Pour chaque noeud l de voisinage N_l:
   *   m = min(N_l)
   *   emet (m, n) pour tout n dans N_l
   *   emet (n, m) pour tout n dans N_l avec n != m
   *   conserve (l, l)
   */
  def ccfIterate(adjacency: RDD[Edge]): RDD[Edge] = {
    val minNeighborByNode: RDD[(Long, Long)] =
      adjacency.reduceByKey((a, b) => math.min(a, b))

    val joined: RDD[(Long, (Long, Long))] = adjacency.join(minNeighborByNode)

    val emitToMin: RDD[Edge] = joined.map {
      case (_, (neighbor, minNeighbor)) => (minNeighbor, neighbor)
    }

    val emitReverse: RDD[Edge] = joined
      .filter { case (_, (neighbor, minNeighbor)) => neighbor != minNeighbor }
      .map { case (_, (neighbor, minNeighbor)) => (neighbor, minNeighbor) }

    val selfLoops: RDD[Edge] = minNeighborByNode.map { case (node, _) => (node, node) }

    emitToMin.union(emitReverse).union(selfLoops)
  }

  /**
   * Conteneur pour la sortie d'une etape de deduplication.
   */
  final case class DedupResult(
      adjacency: RDD[Edge],
      addedEdges: Long,
      removedEdges: Long,
      converged: Boolean
  )

  /**
   * Phase CCF-Dedup:
   * - retire les couples d'arcs dupliques
   * - detecte la convergence en verifiant si l'ensemble d'arcs change
   */
  def ccfDedup(previousAdjacency: RDD[Edge], candidates: RDD[Edge]): DedupResult = {
    val nextAdjacency = candidates.distinct().cache()
    nextAdjacency.count() // materialise le cache une fois

    val addedEdges = nextAdjacency.subtract(previousAdjacency).count()
    val removedEdges = previousAdjacency.subtract(nextAdjacency).count()
    val converged = (addedEdges + removedEdges) == 0L

    DedupResult(nextAdjacency, addedEdges, removedEdges, converged)
  }

  /**
   * Conteneur pour la sortie complete d'execution CCF.
   */
  final case class RunResult(finalAdjacency: RDD[Edge], iterations: Int)

  /**
   * Execute la boucle iterative CCF jusqu'a convergence ou maxIterations.
   */
  def runCCF(
      edges: RDD[Edge],
      maxIterations: Int = 100,
      logProgress: Boolean = true
  ): RunResult = {
    var adjacency = buildInitialAdjacency(edges).cache()
    adjacency.count() // materialise le premier cache

    var iteration = 0
    var converged = false

    while (iteration < maxIterations && !converged) {
      val candidates = ccfIterate(adjacency)
      val dedupResult = ccfDedup(adjacency, candidates)

      adjacency.unpersist(blocking = false)
      adjacency = dedupResult.adjacency

      iteration += 1
      converged = dedupResult.converged

      if (logProgress) {
        println(
          s"[CCF-ScalaRDD] iteration=$iteration " +
            s"added=${dedupResult.addedEdges} removed=${dedupResult.removedEdges} converged=$converged"
        )
      }
    }

    RunResult(adjacency, iteration)
  }

  /**
   * Extrait (node, componentId) depuis l'adjacence finale:
   * componentId est l'etiquette voisine minimale pour chaque noeud.
   */
  def extractComponents(finalAdjacency: RDD[Edge]): RDD[(Long, Long)] = {
    finalAdjacency
      .reduceByKey((a, b) => math.min(a, b))
      .sortByKey(ascending = true)
  }

  /**
   * Helper de bout en bout.
   */
  def connectedComponents(
      edges: RDD[Edge],
      maxIterations: Int = 100,
      logProgress: Boolean = true
  ): RDD[(Long, Long)] = {
    val runResult = runCCF(edges, maxIterations, logProgress)
    extractComponents(runResult.finalAdjacency)
  }

  /**
   * Utilisation CLI:
   *   spark-submit --class CCFScala <jar> <inputEdgesPath> [maxIterations]
   *
   * Format d'entree:
   *   un arc par ligne, separe par virgule ou espace, exemple:
   *   1 2
   *   2,3
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: CCFScala <inputEdgesPath> [maxIterations]")
      System.exit(1)
    }

    val inputPath = args(0)
    val maxIterations = if (args.length >= 2) args(1).toInt else 100

    val spark = SparkSession.builder().appName("CCFScala").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val edges: RDD[Edge] = sc.textFile(inputPath)
      .filter(_.trim.nonEmpty)
      .map { line =>
        val parts = line.trim.split("[,\\s]+")
        require(
          parts.length >= 2,
          s"Ligne d'arc invalide '$line'. Deux champs numeriques sont attendus."
        )
        (parts(0).toLong, parts(1).toLong)
      }

    val components = connectedComponents(edges, maxIterations, logProgress = true)
    components.collect().foreach { case (node, componentId) =>
      println(s"$node,$componentId")
    }

    spark.stop()
  }
}
