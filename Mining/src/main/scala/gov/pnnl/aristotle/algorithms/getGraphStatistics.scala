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
import org.apache.spark.rdd.RDD

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
          
    
      
      /*
       * Get Total Edge of a given edgeType
       */
      val targetEdgeTypes = Array(9)
      targetEdgeTypes.foreach(et=>{
        val totalTargetEdges = typeGraph.triplets.filter(t 
            => t.attr.getlabel == et).count
        outputStatFile.println("Total Edges of Type " + et + " " + totalTargetEdges)
      })
      

      /*
       * Get count of facts which does not has a typed predicate, grouped by 
       * edge type
       * i.e.
       * 		<paper1> <hasConfId>	<c10>  
       *   where there is not type for <c10>
       */
      val unTypedEdgesPerEdgeType = typeGraph.triplets.map(t=>{
        if(t.dstAttr._2.size == 0)
          (t.attr.getlabel, 1)
          else
            (t.attr.getlabel, 0)
      }).reduceByKey((cnt1,cnt2) => cnt1 + cnt2).collect()
      unTypedEdgesPerEdgeType.foreach(et => {
        outputStatFile.println("Total Typeless Edges of Type " + et + " " + et._2)
      })
     
      /*
       * Similarly Get count of facts which DOES has a typed predicate, grouped by 
       * edge type
       * i.e.
       * 		<paper1> <hasConfId>	<c10>  
       *   where there IS type for <c10>
       */
      val typedEdgesPerEdgeType = typeGraph.triplets.map(t=>{
        if(t.dstAttr._2.size > 0)
          (t.attr.getlabel, 1)
          else
            (t.attr.getlabel, 0)
      }).reduceByKey((cnt1,cnt2) => cnt1 + cnt2).collect()
      typedEdgesPerEdgeType.foreach(et => {
        outputStatFile.println("Total Typed Edges of Type " + et + " " + et._2)
      })
     
      
      
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