import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * CCF (Connected Components via MapReduce) implemented with Spark RDD in Scala.
 *
 * Input:
 *   RDD[(src, dst)] where each tuple is an edge.
 *
 * Output:
 *   RDD[(node, componentId)] where componentId is the minimum node id of the component.
 */
object CCFScala {

  type Edge = (Long, Long)

  /**
   * Convert input edges into an undirected relation and remove duplicates.
   * This mirrors the graph normalization used in the PySpark DataFrame version.
   */
  def normalizeEdges(edges: RDD[Edge]): RDD[Edge] = {
    val directed = edges.flatMap {
      case (src, dst) if src == dst => Seq((src, dst))
      case (src, dst)               => Seq((src, dst), (dst, src))
    }
    directed.distinct()
  }

  /**
   * Build the (node, neighbor) adjacency relation with explicit self-loops.
   * Self-loops guarantee each node remains present in every iteration.
   */
  def buildInitialAdjacency(edges: RDD[Edge]): RDD[Edge] = {
    val undirected = normalizeEdges(edges)
    val nodes = undirected.flatMap { case (src, dst) => Seq(src, dst) }.distinct()
    val selfLoops = nodes.map(node => (node, node))
    undirected.union(selfLoops).distinct()
  }

  /**
   * CCF-Iterate phase.
   *
   * For each node l with neighborhood N_l:
   *   m = min(N_l)
   *   emit (m, n) for all n in N_l
   *   emit (n, m) for all n in N_l where n != m
   *   keep (l, l)
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
   * Container for one deduplication step output.
   */
  final case class DedupResult(
      adjacency: RDD[Edge],
      addedEdges: Long,
      removedEdges: Long,
      converged: Boolean
  )

  /**
   * CCF-Dedup phase:
   * - remove duplicate edge pairs
   * - detect convergence by checking whether edge-set changed
   */
  def ccfDedup(previousAdjacency: RDD[Edge], candidates: RDD[Edge]): DedupResult = {
    val nextAdjacency = candidates.distinct().cache()
    nextAdjacency.count() // materialize cache once

    val addedEdges = nextAdjacency.subtract(previousAdjacency).count()
    val removedEdges = previousAdjacency.subtract(nextAdjacency).count()
    val converged = (addedEdges + removedEdges) == 0L

    DedupResult(nextAdjacency, addedEdges, removedEdges, converged)
  }

  /**
   * Container for full CCF execution output.
   */
  final case class RunResult(finalAdjacency: RDD[Edge], iterations: Int)

  /**
   * Run CCF iterative loop until convergence or maxIterations.
   */
  def runCCF(
      edges: RDD[Edge],
      maxIterations: Int = 100,
      logProgress: Boolean = true
  ): RunResult = {
    var adjacency = buildInitialAdjacency(edges).cache()
    adjacency.count() // materialize first cache

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
   * Extract (node, componentId) from final adjacency:
   * componentId is the minimum neighbor label for each node.
   */
  def extractComponents(finalAdjacency: RDD[Edge]): RDD[(Long, Long)] = {
    finalAdjacency
      .reduceByKey((a, b) => math.min(a, b))
      .sortByKey(ascending = true)
  }

  /**
   * End-to-end helper.
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
   * CLI usage:
   *   spark-submit CCFScala.scala <inputEdgesPath> [maxIterations]
   *
   * Input format:
   *   one edge per line, separated by comma or whitespace, example:
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
          s"Invalid edge line '$line'. Expected two numeric fields."
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
