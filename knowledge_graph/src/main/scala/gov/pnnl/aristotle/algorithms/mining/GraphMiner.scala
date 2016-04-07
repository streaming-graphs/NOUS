package gov.pnnl.aristotle.algorithms.mining

import org.apache.spark.graphx.Graph
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.PrintWriter
import java.io.File
import scala.util.control.Breaks._
import scalaz.Scalaz._
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternDependencyGraph
import gov.pnnl.aristotle.algorithms.ReadHugeGraph

/**
 * @author puro755
 * Entry point to the incremental graph mining on a sliding window graph.
 * The updated version of the graph mining algorithm continuously reads every
 * new file/folder and treat it as a new input batch graph.    It construct a
 * new graph as a result of the vertex intersection of the     exiting window
 * graph and incoming batch graph. This new graph is used   as the base graph
 * for the graph mining operation. The graph mining   operation iterates over
 * the graph and collect frequent patterns. In each iteration it filters out
 * non-frequent patterns and propagate only the frequent patterns.
 */
  
object GraphMiner {

  /*
   * Initialize the spark context and its run-time configurations
   */

  val sparkConf = new SparkConf().setAppName("NOUS Graph Pattern Miner").setMaster("local")
    .set("spark.rdd.compress", "true").set("spark.serializer",
      "org.apache.spark.serializer.KryoSerializer")

  sparkConf.registerKryoClasses(Array.empty)
  val sc = new SparkContext(sparkConf)

  /*
   * Initialize Log File Writer & Set up the Logging level
   */
  val writerSG = new PrintWriter(new File("GraphMiningOutput.txt"))
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    var t_sc0 = System.nanoTime();

    /*
	 * Read multiple files. for now, each file is treated as a new batch.
	 */
    val minSup = args(1).toInt;
    val format: String = args(4)
    val number_of_input_files = args.size
    val windowSize = 5
    var batch_id = -1;
    var gWin = new DynamicPatternGraphV2Flat(minSup)
    var gDep = new PatternDependencyGraph
    var pattern_trend: Map[String, List[(Int, Int)]] = Map.empty

    /*
     * Read all the files/folder one-by-one and construct an input graph
     */
    for (i <- 4 to number_of_input_files - 1) {

      /*
       * Count the start time of the batch processing
       */
      val t_b0 = System.nanoTime();
      var t0 = System.nanoTime();
      val input_graph = getGraph(batch_id, i, args)
      var t1 = System.nanoTime();
      println("#Time to load the  graph" + " =" + (t1 - t0) * 1e-9 + "seconds," +
        "#First Vertex is" + " =" )
      
        
        
      /*
       *  gBatch is a pre-processed version of input graph. It has 1 edge 
       *  pattern on each vertex. all the other information is removed from the
       *  vertex. 
       */
      val gBatch = new DynamicPatternGraphV2Flat(minSup).init(input_graph, 
          writerSG,args(0),args(2).toInt)
      //gBatch.input_graph.vertices.collect.foreach(f => println("vertex " +f.toString))
      gWin.trim(i, windowSize)
      val batch_window_intersection_graph = gWin.merge(gBatch, sc)
      var level = 0;
      val iteration_limit: Int = args(3).toInt
      batch_window_intersection_graph.input_graph.vertices.collect.foreach(f
          =>writerSG.println(f))
      writerSG.flush()
      breakable {
        while (1 == 1) {
          level = level + 1
          println(s"#####Iteration ID $level and interation_limit is $iteration_limit")
          val t_i0 = System.nanoTime()
          if (level == iteration_limit) break;

          /*
           * calculate the join time for the main mining operation
           */
          val t_j0 = System.nanoTime()
          batch_window_intersection_graph.input_graph =
            batch_window_intersection_graph.joinPatterns(gDep, writerSG, level)
          val t_j1 = System.nanoTime()
          println("#Time to get join the patterns" + " =" + (t_j1 - t_j0) * 1e-9 + "seconds," +
            "#TSize of first vertext of  window graph" + " =" )
 
          /*
           * Update the dependency graph 
           */
          val t_d0 = System.nanoTime()
          gDep.graph = gWin.updateGDep(gDep)
          val t_d1 = System.nanoTime()
          println("#Time to get update dependency graph" + " =" + (t_d1 - t_d0) * 1e-9 + "seconds," +
            "#TSize of first vertext of  dependency graph" + " =")

          println("updating gDep DONE")
          val t_i1 = System.nanoTime()
          println("#Time to mine the iteration" + " =" + (t_i1 - t_i0) * 1e-9 + "seconds," +
            "#TSize of  pattern_trend map" + " =" + pattern_trend.size)

          /*
             * Update the current graph by removing special purpose '|' symbols 
             * in the pattern keys. This symbol is used to identify which small
             * patterns participate in construction a bigger pattern. 
             */
          batch_window_intersection_graph.input_graph =
            GraphPatternProfiler.get_Frequent_SubgraphV2Flat(
              GraphPatternProfiler.fixGraphV2Flat(batch_window_intersection_graph.input_graph), null, minSup)
        }
      }
 
      /*
       * Now merger the mined intersection graph with original window
       */
      gWin.input_graph = gWin.mergeBatchGraph(batch_window_intersection_graph.input_graph)

      val pattern_in_this_batch = GraphPatternProfiler.get_sorted_patternV2Flat(gWin.input_graph,
        writerSG, 2, args(1).toInt)
      pattern_in_this_batch.collect.foreach(f => writerSG.println("pt = " + f.toString))

      val t_b1 = System.nanoTime();
      println("#Time to mine the batch" + " =" + (t_b1 - t_b0) * 1e-9 + "seconds," +
        "#TSize of  edge_join update graph" + " =" + pattern_trend.size)
    }
    var t_sc1 = System.nanoTime();
    println("#Time to load the  graph" + " =" + (t_sc1 - t_sc0) * 1e-9 + "seconds," +
      "#TSize of  edge_join update graph" + " =" + pattern_trend.size)
    
     /*
     * Stop the spark context
     */ 
   sc.stop
  }

  //TODO: move it to ReadHugeGraph without batch id
  def getGraph(batch_id: Long, i: Int, args: Array[String]): Graph[String, KGEdge] =
    {
      val new_batch_id = batch_id + 1
      var multi_edge_graph: Graph[String, KGEdge] = null
      if (args(i).endsWith(".obj"))
        multi_edge_graph = ReadHugeGraph.getGraphObj_KGEdge(args(i) + "/vertices", args(i) + "/edges", sc)
      else if (args(i).endsWith(".lg"))
        multi_edge_graph = ReadHugeGraph.getGraphLG_Temporal(args(i), sc)
      else if(args(i).endsWith(".els"))
        multi_edge_graph = ReadHugeGraph.getGraphElsevier_Temporal(args(i), sc) 
      else
        multi_edge_graph = ReadHugeGraph.getTemporalGraph(args(i), sc)

        return multi_edge_graph.subgraph( vpred = (vid, attr) => attr != null )
    }
}