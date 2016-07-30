package gov.pnnl.aristotle.algorithms

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
import gov.pnnl.aristotle.algorithms.mining.GraphPatternProfiler
import org.apache.commons.io.FilenameUtils
import org.apache.spark.rdd.RDD
import gov.pnnl.aristotle.algorithms.mining.v3.CandidateGeneration
import gov.pnnl.aristotle.algorithms.mining.v3.WindowStateV3
import gov.pnnl.aristotle.algorithms.mining.v3.BatchMetrics
import gov.pnnl.aristotle.algorithms.mining.v3.WindowMetrics
import scala.io.Source

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
   * Initialize the spark context and its run-time configurations.
   * Remove .setMaster("local") before running this on a cluster
   */

  val sparkConf = new SparkConf()
    .setAppName("NOUS Graph Pattern Miner")
    .set("spark.rdd.compress", "true")
    .set("spark.shuffle.blockTransferService", "nio")
    
    

  sparkConf.registerKryoClasses( Array.empty )
  val sc = new SparkContext( sparkConf )

  /*
   * Initialize Log File Writer & Set up the Logging level
   */
  val writerSG = new PrintWriter( new File( "GraphMiningOutput.txt" ) )
  Logger.getLogger( "org" ).setLevel( Level.OFF )
  Logger.getLogger( "akka" ).setLevel( Level.OFF )

  def main( args : Array[String] ) : Unit = {

    var t_sc0 = System.nanoTime();

    if(args.length != 5) {
      println("Usage: <edge label denoting node type(Int)> <minSupport> " + 
          " <type threshold for node> <numIterations> "  + 
          " <BatchInfoFile (containingPathToDataDirectoriesPerBatch)>")
      exit
    }
    /*
	 * Read multiple files. Each file is treated as a new batch.
	 */
    val baseEdgeType = args(0).toInt
    val minSup = args(1).toInt;
    val nodeTypeThreshold = args(2).toInt
    val numIter = args(3).toInt
    val listBatchFiles = args(4)
    val windowSize = 5
    val window = new WindowStateV3()
    val window_metrics = new WindowMetrics()

    var batch_id = -1;
    var gWin = new CandidateGeneration( minSup )
    var pattern_trend : Map[String, List[( Int, Int )]] = Map.empty

    /*
     * Read ` the files/folder one-by-one and construct an input graph
     */
    for (
      filepath <- Source.fromFile(listBatchFiles).
        getLines().filter(str => !str.startsWith("#"))
    ) {

    /*
     * batch_id: each files is read as a new batch.
     */
      batch_id = batch_id + 1
      println(s"******Reading File $filepath with batch_id= $batch_id")
      val batch_metrics = new BatchMetrics( batch_id )

      //val input_graph = ReadHugeGraph.getGraphFileTypeInt( filepath, sc )
      val input_graph = ReadHugeGraph.getGraphFileTypeInt( filepath, sc )

      /*
       *  gBatch is a pre-processed version of input graph. It has 1 edge 
       *  pattern on each vertex. all the other information is removed from the
       *  vertex. 
       *  
       *  gBatch is has all the one edge patterns on every node
       *  Example : (2101833240, Map(List(48, 2, 1516351, -1) -> 1, List(48, 3, 0, -1) -> 1))
       *  
       *  2101833240: VertexId of the node
       *  List(48, 2, 1516351, -1) : is the pattern key. At this point of the algorithm
       *  every key ends with -1 that is used to construct bigger size patterns.
       *  
       *  -> 1 : is local support of the pattern on this node only (thats why it is called local support) 
       */
      val gBatch = new CandidateGeneration(minSup).init(sc, input_graph, writerSG, baseEdgeType, nodeTypeThreshold)
      
      /*
       *  Update the batch_id with its min/max time
       */
      //val batch_min_max_time = gBatch.getMinMaxTime()
      //gWin.batch_id_map + ( batch_id -> batch_min_max_time )
      //gWin.trim(i, windowSize)

      /*
       * batch_window_intersection_graph is a common graph between gBatch and 
       * gWindow. This is the graph which is mined by the algorithm. It includes
       * all the patterns of the boundary nodes already minded in the gWindow.
       */
      val batch_window_intersection_graph = gWin.getInterSectionGraph( gBatch, sc )
      batch_window_intersection_graph.input_graph =
        gWin.filterNonPatternSubGraphV2( batch_window_intersection_graph.input_graph )

        var level = 0;
      
      writerSG.flush()
      breakable {
        while ( 1 == 1 ) {
          level = level + 1
          //println( s"#####Iteration ID $level and interation_limit is $iteration_limit" )
          if ( level > numIter ) break;

          batch_window_intersection_graph.input_graph = batch_window_intersection_graph.joinPatterns( writerSG, level )
          /*
           * Update the dependency graph 
           */
          window.updateGDep( batch_window_intersection_graph.input_graph, baseEdgeType , minSup)
          /*
             * Update the current graph by removing special purpose '|' symbols 
             * in the pattern keys. This symbol is used to identify which small
             * patterns participate in construction a bigger pattern. 
             */
          batch_window_intersection_graph.input_graph =
            GraphPatternProfiler.get_Frequent_SubgraphV2Flat(sc,
              GraphPatternProfiler.fixGraphV2Flat( batch_window_intersection_graph.input_graph ), null, minSup )
          val pat2 = batch_window_intersection_graph.input_graph.vertices.map(v=>{
       (v._1, v._2.getpattern_map.size)
      })
      pat2.saveAsTextFile("pat2"+System.nanoTime())
        }
      }

      /*
       * Now merge the mined intersection graph with original window
       */
      window.mergeBatchGraph( batch_window_intersection_graph.input_graph )
      batch_metrics.updateBatchMetrics( batch_window_intersection_graph.input_graph, writerSG, args )
      window_metrics.updateWindowMetrics( batch_metrics )

//      val infrequent = window.gDep.graph.vertices.filter(v=>v._2.getptype == -1).count
//      val promising = window.gDep.graph.vertices.filter(v=>v._2.getptype == 0).count
//      val closed = window.gDep.graph.vertices.filter(v=>v._2.getptype == 1).count
//      val redundant = window.gDep.graph.vertices.filter(v=>v._2.getptype == 2).count
//      val frequent = promising + closed + redundant
//      
//      println("pattern type infreqent, promising, closed, redundant, frequent ie. pro+clo+red" , infrequent, " " , promising, " " , closed, " ", redundant, " " , frequent)
      writerSG.flush()
    }

    println("saving window")
    
    window_metrics.saveWindowMetrics()
    window.saveDepG

    var t_sc1 = System.nanoTime();
    println( "#Time to load the  graph" + " =" + ( t_sc1 - t_sc0 ) * 1e-9 + "seconds," +
      "#TSize of  edge_join update graph" + " =" + pattern_trend.size )

    /*
     * Stop the spark context
     */
    sc.stop
  }

}