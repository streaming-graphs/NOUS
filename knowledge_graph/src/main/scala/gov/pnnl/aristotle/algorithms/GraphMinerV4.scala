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
import gov.pnnl.aristotle.algorithms.mining.v3.CandidateGenerationV4
import java.io.File


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
    //var pattern_trend : Map[String, List[( Int, Int )]] = Map.empty

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
      //val batch_metrics = new BatchMetrics( batch_id )

      val input_graph = ReadHugeGraph.getGraphFileTypeInt(filepath, sc)

      t_sc0 = System.nanoTime()
      val batch_graph = new CandidateGenerationV4(minSup)
      val GIP = batch_graph.init(sc, input_graph, writerSG, baseEdgeType, nodeTypeThreshold)

      val misPatternSupport = batch_graph.computeMinImageSupport(GIP)
      
      
      val gip_vertices_4degree = GIP.degrees.filter(v => v._2 == 4).count
      val gip_vertices_1degree = GIP.degrees.filter(v => v._2 == 1).count
      val gip_vertices_2degree = GIP.degrees.filter(v => v._2 == 2).count
      val gip_vertices_3degree = GIP.degrees.filter(v => v._2 == 3).count

      println("total GIP nodes", GIP.vertices.count)
      println("nodes with 1 degree = ", gip_vertices_1degree)
      println("nodes with 2 degree = ", gip_vertices_2degree)
      println("nodes with 3 degree = ", gip_vertices_3degree)
      println("nodes with 4 degree = ", gip_vertices_4degree)
      
      GIP.vertices.saveAsObjectFile("GIP/vertices/"+System.nanoTime())
      GIP.edges.saveAsObjectFile("GIP/edges/"+System.nanoTime())
      misPatternSupport.saveAsObjectFile("GIP/misPattenSupport"+System.nanoTime())
    }

  }

}