/**
 *
 * @author puro755
 * @dAug 22, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms

import java.io.PrintWriter
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import gov.pnnl.aristotle.algorithms.mining.v4.CandidateGenerationV4
import java.io.File
import org.apache.spark.graphx.Graph
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternInstanceNode
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt


/**
 * @author puro755
 *
 */
object GraphMinerV4 {

  /*
   * Initialize the spark context and its run-time configurations.
   * Remove .setMaster("local") before running this on a cluster
   */

  val sc = SparkContextInitializer.sc

  /*
   * Initialize Log File Writer & Set up the Logging level
   */
  val writerSG = new PrintWriter(new File("GraphMiningOutputV4.txt"))
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    var t_sc0 = System.nanoTime();

    if (args.length != 5) {
      println("Usage: <edge label denoting node type(Int)> <minSupport> " +
        " <type threshold for node> <numIterations> " +
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
    //    val window = new WindowStateV3()
    //    val window_metrics = new WindowMetrics()

    var batch_id = -1;
    var gWin = new CandidateGenerationV4(minSup)
    var winodow_GIP : Graph[PatternInstanceNode, Long] = null
    //var pattern_trend : Map[String, List[( Int, Int )]] = Map.empty

    /*
     * Read ` the files/folder one-by-one and construct an input graph
     */
    for (filepath <- Source.fromFile(listBatchFiles).
        getLines().filter(str => !str.startsWith("#"))) {
     /*
     * batch_id: each files is read as a new batch.
     */
      batch_id = batch_id + 1
      println(s"******Reading File $filepath with batch_id= $batch_id")
      //val batch_metrics = new BatchMetrics( batch_id )

      /*
       * input_graph is the raw batch graph.
       */
      val input_graph : Graph[Int, KGEdgeInt] = ReadHugeGraph.getGraphFileTypeInt(filepath, sc)

      t_sc0 = System.nanoTime()
      val batchGraphGenerator = new CandidateGenerationV4(minSup)
      winodow_GIP = batchGraphGenerator.init(sc, input_graph, writerSG, 
          baseEdgeType, nodeTypeThreshold,winodow_GIP)
      
      val misPatternSupport = batchGraphGenerator.computeMinImageSupport(winodow_GIP)
      misPatternSupport.saveAsTextFile("GIP/misPattenSupport"+System.nanoTime())
    }

  }

}