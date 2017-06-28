/**
 *
 * @author puro755
 * @dJun 27, 2017
 * @Mining
 */
package gov.pnnl.aristotle.algorithms

import org.ini4j.Wini
import java.io.File
import org.joda.time.format.DateTimeFormat
import scala.io.Source
import org.apache.spark.graphx.Graph
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.VertexRDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.JavaConversions._
import java.io.FileWriter

/**
 * @author puro755
 *
 */
object getGraphStatistics {

  def main(args: Array[String]): Unit = {

    val sc = SparkContextInitializer.sc
    type DataGraph = Graph[Int, KGEdgeInt]
    type NbrTypes = Set[Int]
    /*
     * Read configuration parameters.
     * Please change the parameter in the conf file 'args(0)'. sample file is conf/knowledge_graph.conf
     */
    val confFilePath = args(0)
    val ini = new Wini(new File(confFilePath));
    val pathOfBatchGraph = ini.get("run", "batchInfoFilePath");
    val startTime = ini.get("run", "startTime").toInt
    val batchSizeInTime = ini.get("run", "batchSizeInTime")
    val typePred = ini.get("run", "typeEdge").toInt
    val dateTimeFormatPattern = ini.get("run", "dateTimeFormatPattern")
    val EdgeLabelDistributionDir = ini.get("output", "EdgeLabelDistributionDir")
    val allAttributeEdgesLine = ini.get("run", "attributeEdge");
    val allAttributeEdges: Array[Int] = allAttributeEdgesLine.split(",").map(_.toInt)

    val batchSizeInMilliSeconds = getBatchSizerInMillSeconds(batchSizeInTime)
    var currentBatchId = getBatchId(startTime, batchSizeInTime) - 1

    val op  = "Graph.stat"
    val outputStatFile = new PrintWriter(new File(op))
    
    for (
      graphFile <- Source.fromFile(pathOfBatchGraph).
        getLines().filter(str => !str.startsWith("#"))
    ) {
      
      
      
      var t0_batch = System.nanoTime()

      currentBatchId = currentBatchId + 1
      var t0 = System.nanoTime()
      val incomingDataGraph: DataGraph = ReadHugeGraph.getTemporalGraphInt(graphFile,
        sc, batchSizeInMilliSeconds, dateTimeFormatPattern).cache

      val typeGraph = DataToPatternGraph.getTypedGraph(incomingDataGraph, typePred)
      
      
      /*
       * Get Total Number of nodes and edges 
       */
      val totalNodes = typeGraph.vertices.count
      val totalEdges = typeGraph.edges.count
      outputStatFile.println("Total Nodes=" + totalNodes)
      outputStatFile.println("Total Edges=" + totalEdges)
      
      
      /*
       * Get Total typed Nodes
       */
      val totalTypedNodes = typeGraph.vertices.filter(v=>v._2._2.size > 0).count
      val totalTypedEdges = typeGraph.triplets.filter(t
          =>((t.srcAttr._2.size > 0) && (t.dstAttr._2.size > 0))).count
      outputStatFile.println("Total Typed Nodes=" + totalTypedNodes)
      outputStatFile.println("Total Typed Edges=" + totalTypedEdges)
          
    
      
      
      
    
      outputStatFile.flush()
    }

  }

  def getBatchSizerInMillSeconds(batchSize: String): Long =
    {
      val MSecondsInYear = 31556952000L
      if (batchSize.endsWith("y")) {
        val batchSizeInYear: Int = batchSize.replaceAll("y", "").toInt
        return batchSizeInYear * MSecondsInYear
      }
      return MSecondsInYear
    }

  def getBatchId(startTime: Int, batchSizeInTime: String): Int =
    {
      val startTimeMilliSeconds = getStartTimeInMillSeconds(startTime)
      val batchSizeInTimeIntMilliSeconds = getBatchSizerInMillSeconds(batchSizeInTime)
      return (startTimeMilliSeconds / batchSizeInTimeIntMilliSeconds).toInt
    }
  def getStartTimeInMillSeconds(startTime: Int): Long =
    {
      val startTimeString = startTime + "/01/01 00:00:00.000"
      val f = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS");
      val dateTime = f.parseDateTime(startTimeString);
      return dateTime.getMillis()
    }
}