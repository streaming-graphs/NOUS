package gov.pnnl.aristotle.algorithms.mining

import java.io.PrintWriter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import org.apache.spark.graphx.Graph
import scalaz.Scalaz._
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import gov.pnnl.aristotle.utils.Gen_Utils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import gov.pnnl.aristotle.algorithms.mining.datamodel.BatchState
import gov.pnnl.aristotle.algorithms.mining.datamodel.WindowState
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternInstance
import gov.pnnl.aristotle.algorithms.mining.datamodel.PGNode
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge

object GraphFrequentPatternMining {

  def main(args: Array[String]): Unit = {

    /*
     * Init Spark Context
     */
    val t00 = System.currentTimeMillis();
    val writerSG = new PrintWriter(new File("GraphMiningOutput.txt" + System.nanoTime()))
    val sparkConf = new SparkConf().setAppName("Load Huge Graph Main").
      set("spark.rdd.compress", "true")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val number_of_input_files = args.size

    /*
	 * Initialize Global Window State which will get updated on each file read
	 */
    var graph_dependency_nodes: RDD[(VertexId, PGNode)] = sc.emptyRDD
    // Create some empty RDDs
    var relationships: RDD[Edge[Int]] = sc.emptyRDD
    var dependency_graph: Graph[PGNode, Int] = Graph(graph_dependency_nodes,
      relationships);
    var sub_graph: RDD[(String, Set[PatternInstance])] = sc.emptyRDD
    var closed_patterns_graph: Graph[PGNode, Int] = null;
    var redundent_patterns_graph: Graph[PGNode, Int] = null;
    var promising_patterns_graph: Graph[PGNode, Int] = null;
    var pattern_trend: Map[String, List[(Int, Int)]] = Map.empty

    var window_state = new WindowState(sub_graph, dependency_graph,
      redundent_patterns_graph, closed_patterns_graph,
      promising_patterns_graph, pattern_trend)

    /*
	 * Read multiple files. for now, each file is treated as a new batch.
	 */
    var batch_id = -1;
    val format: String = args(4)
    for (i <- 4 to number_of_input_files - 1) {
      batch_id += 1
      println("Reading graph Start" + i + "   " + number_of_input_files)
      var t0 = System.nanoTime();
      var multi_edge_graph: Graph[String, KGEdge] = null
      if (args(i).endsWith(".obj"))
        multi_edge_graph = ReadHugeGraph.getGraphObj_KGEdge(args(i) + "/vertices",
          args(i) + "/edges", sc)
      else if (args(i).endsWith(".lg"))
        multi_edge_graph = ReadHugeGraph.getGraphLG_Temporal(args(i), sc)
      else
        multi_edge_graph = ReadHugeGraph.getTemporalGraph(args(i), sc)

      val batch_state = new BatchState(multi_edge_graph, 0L, 0L, batch_id)
      var graph = multi_edge_graph.groupEdges((edge1, edge2) => edge1)
      var t1 = System.nanoTime();
      writerSG.println("#####Time to load graph" + " =" + (t1 - t0) * 1e-9 +
        "s" + graph.vertices.count)
      println("done" + args(0));

      t0 = System.nanoTime()

      Gen_Utils.time(window_state = GraphPatternProfiler.PERCOLATOR(batch_state,
        sc, writerSG, args(0), args(1).toInt, args(2).toInt, args(3).toInt,
        window_state),
        "PERCOLATOR")
      t1 = System.nanoTime()
      writerSG.println("#####Time To Mine the Graph: " + " =" +
        (t1 - t0) * 1e-9 + "s " + batch_state.getinputgraph.vertices.count)
      println("done with multiple edges file name" + args(i))
      println("original graph vcount=" + multi_edge_graph.vertices.count +
        " and edge-count= " + multi_edge_graph.edges.count)
      writerSG.flush();
    }

    window_state.pattern_trend.foreach(pattern => {
      writerSG.print(pattern._1 + "=>")
      pattern._2.foreach(pattern_entry =>
        writerSG.print("\t" + pattern_entry._1 + ":" + pattern_entry._2))
    })
    writerSG.flush();
  }
}