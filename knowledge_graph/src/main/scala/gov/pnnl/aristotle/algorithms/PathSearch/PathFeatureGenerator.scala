package gov.pnnl.aristotle.algorithms.PathSearch

import scala.io.Source
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.graphx._
import scala.math.Ordering
import breeze.stats.distributions
import scala.util.Sorting
import java.io._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import gov.pnnl.aristotle.utils._
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import org.apache.spark.graphx.Graph.graphToGraphOps

object PathFeatureGenerator {

  /* This class aims to represents graph based feature for every node */
  class NodeFeatureVector(val degree: Int, val nodeType: String) extends Serializable {
    override def toString(): String = {
      nodeType + "\t" + degree.toString 
    }
  }
  
  class MaxDegreeWithTypeFilter(maxDegree:Int, vertexType :String = "ALL") extends 
  NodeFilter[ExtendedVD[String, NodeFeatureVector]] {
    override def isMatch(targetNode: ExtendedVD[String, NodeFeatureVector]) : Boolean = {
      targetNode.extension match {
        case Some(extension) => ((extension.degree <= maxDegree && vertexType == "ALL") ||
              (extension.degree <= maxDegree && extension.nodeType.contains(vertexType)) )    
        case _ => true
      }
    }
  }
  /* This class is a dummy filter which lets all paths(nodes in path) pass the check */ 
  //class FeatureGenerator() extends NodeFilter[ExtendedVD[String, FeatureVector]]
   
  def main(args: Array[String]): Unit = {     
    val sparkConf = new SparkConf().setAppName("run path search interactive").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\fake_hadoop\\")
    println("starting from main")
    if(args.length != 5) {
      println("Usage <Graph Path> " + "<Output Directory>" +
          "<maxpathSize> <degreeFilter> <edgeCountDampingFactor>")
      exit
    }
    
    val graphFile = args(0)
    val outputDir = args(1)
    //val questionsFile = args(1)
    val sizeLongestAnswer = args(2).toInt
    val maxDegree = args(3).toInt
    val edgeCountDampingFactor = args(4).toInt
    
    //saveFeatureRDD(graphFile, outputDir, sc)
    //savePageRank(graphFile, outputDir, sc)
    saveDegree(graphFile, outputDir, sc)
    //saveEdgeRankPerVertex(graphFile, outputDir, sc)
  }
  
  def saveDegree(graphFile: String, outputDir: String, sc: SparkContext): Unit = {
    val g: Graph[String, String] = ReadHugeGraph.getGraph(graphFile, sc)
    val degree = g.degrees
    val degreeWithLabel = degree.join(g.vertices)
    val degreeWithLabelOnly = degreeWithLabel.map(v => (v._2._2, v._2._1))
    degreeWithLabel.saveAsTextFile(outputDir + "/vertices.degrees.txt")
  }
  
  def savePageRank(graphFile: String, outputDir: String, sc: SparkContext): Unit = 
  {
   val g: Graph[String, String] = ReadHugeGraph.getGraph(graphFile, sc)
   println(" calculating page ranks")
   val pageRankGraph: Graph[Double, Double] = g.pageRank(0.01)
   val minRank = pageRankGraph.vertices.values.min
   val maxRank = pageRankGraph.vertices.values.max
   val range = maxRank-minRank
   if(range == 0){
     println("somethign went wrong in pagerank, all ranks are same")
     exit
   }
   
   println("Max page rank and min rank", maxRank, minRank)
   println("Saving page rank of vertices")
   val verticesRank = g.vertices.innerZipJoin(pageRankGraph.vertices)(
       (id, label, rank) => (label, rank))      
  /*     
   *  Note: pagerank calculates edge weight as edgeRank = 1/outdegree_srcVertex
   */ 
   verticesRank.map(idWithLabelAndRank =>
       (idWithLabelAndRank._2._1, idWithLabelAndRank._2._2))
       .saveAsTextFile(outputDir + "/vertices.pagerank.txt") 
  }
  
  
  def saveEdgeRankPerVertex(graphFile: String, outputDir: String, sc: SparkContext): 
  Unit = {
   val g: Graph[String, String] = ReadHugeGraph.getGraph(graphFile, sc)   
   println(" calculating edge ranks per vertex")
   
   val verticesWithEdgeTypes: VertexRDD[Array[String]] = g.aggregateMessages[Array[String]](edge =>
     {
       edge.sendToSrc(Array(edge.attr))
       edge.sendToDst(Array(edge.attr))
     }, 
     (label1, label2) => label1 ++ label2
     )
     
    val vertexWithEdgeCounts: VertexRDD[Map[String, Int]] = 
      verticesWithEdgeTypes.mapValues(edgeLabels => 
        edgeLabels.groupBy(l => l).map(v => (v._1, v._2.size)))
    vertexWithEdgeCounts.take(5).foreach(node => println(node._1, node._2)) 
 /*       
    val normalizedEdgeWeights = vertexWithEdgeCounts.mapValues(edgeLabelsWithCounts =>
      {
        val numUniqueEdges = edgeLabelsWithCounts.size
        edgeLabelsWithCounts.mapValues(freq => 1/(freq*numUniqueEdges))
      })
      
     normalizedEdgeWeights.take(5).foreach(node => println(node._1, node._2)) 
   */     
  }
  
  def saveFeatureRDD(graphFile: String, outputDir: String, sc: SparkContext): Unit = {
    
    val g: Graph[String, String] = ReadHugeGraph.getGraph(graphFile, sc)
    val vertexDegreeRDD = g.degrees
    val vertexTypeRDD: VertexRDD[String] = NodeProp.getNodeType(g)
    
   val vertexFeatures = g.vertices.leftZipJoin(vertexDegreeRDD)((id, label, degree) => (label, degree.getOrElse(0)))
   .leftZipJoin(vertexTypeRDD)((id, labelWithDegree, vertexType) => 
     (labelWithDegree._1, labelWithDegree._2, vertexType.getOrElse("NONE")))
     
   vertexFeatures.saveAsObjectFile(outputDir + "/vertexFeatures.yago2_facts_wiki_type.obj")
   vertexFeatures.map(v => (v._2._1, v._2._2, v._2._3)).
   saveAsTextFile(outputDir + "/vertexFeatures.yago2_facts_wiki_type.txt")
  }
  
  def writePathProfile[VD, ED](allPaths: List[List[PathEdge]], 
      filename: String, vertexIdData:Map[Long, ExtendedVD[String, NodeFeatureVector]], 
      edgeLabelData: Map[String, Long]): Unit = {
    println(" Saving path profile = ", filename)
    if(allPaths.length > 0) {
       val pathString: String = allPaths.map(path =>
         path.map(pathedge => {
             val srcData = getVertexData(vertexIdData, pathedge.srcId)
             val dstData = getVertexData(vertexIdData, pathedge.dstId)
             val edgeData = getEdgeData(edgeLabelData, pathedge.edgeLabel)
              if(pathedge.isOutgoing){
                "[ " + srcData + " ] \t [" + edgeData + " ] \t [ " + dstData + " ]"
              } else {
                 "[ " + dstData + " ] \t [" + edgeData + " ] \t [ " + srcData + " ]"
              }
            }).
            reduce((edge1String, edge2String) => edge1String + "\n" + edge2String)).
            reduce((path1String, path2String) => path1String + "\n\n" + path2String)
       
       Gen_Utils.writeToFile(filename, pathString)
     }
  }
 
 
  
  def getVertexData[VD](vertexIdData:Map[Long, ExtendedVD[String, NodeFeatureVector]], id: Long) : String = {
      val feature = vertexIdData.get(id)  
      feature match {
        case Some(data) => data.extension.get.toString
        case _ => ""
      }
  }
  
  def getEdgeData[ED](edgeLabelData: Map[String, Long], label: String): String = {
    if(edgeLabelData.contains(label))
      edgeLabelData.get(label).toString()
    else
      "None"  
  }
  
  def getTypedAndAliasedGraph(g: Graph[String, String]): Graph[String, String] = {
    val verticesWithType = NodeProp.getNodeType(g)
    val verticesWithAlias = NodeProp.getNodeAlias(g)
    g.joinVertices(verticesWithAlias)((id, label, alias) => {
      //println(" LABEL =", label, " Type Data =", typeInfoString)
      (label ++ "__" + alias)
    }).joinVertices(verticesWithType)((id, label, typeInfoString) => {
      //println(" LABEL =", label, " Type Data =", typeInfoString)
      (label ++ "__" + typeInfoString)
    })
  }
   
  def getTypedGraph(g: Graph[String, String]): Graph[String, String] = {
    val verticesWithType = NodeProp.getNodeType(g) 
    g.joinVertices(verticesWithType)((id, label, typeInfoString) => {
      //println(" LABEL =", label, " Type Data =", typeInfoString)
      (label ++ "__" + typeInfoString)
    })
  }
    
  def getAliasedGraph(g: Graph[String, String]): Graph[String, String] = {
    val verticesWithAlias = NodeProp.getNodeAlias(g)
    g.joinVertices(verticesWithAlias)((id, label, alias) => {
      //println(" LABEL =", label, " Type Data =", typeInfoString)
      (label ++ "__" + alias)
    })
  }
   
  def saveEdgeDistributionRank[VD](g:Graph[VD, String], dampingFactor:Int, outputDir:String): Map[String, Long] = {    
     val edgeLabel: RDD[String] = g.edges.map(edge => edge.attr)
     val edgeLabelCounts = edgeLabel.countByValue
     
     val outputFile = new File(outputDir + "edgeCounts.txt")
     val outputFileWriter = new BufferedWriter(new FileWriter(outputFile))
     
     println("Saving actual edge label counts to edgeCounts.txt ")
     //edgeLabelCounts.foreach(edgeLabelCount=> println(edgeLabelCount._1, edgeLabelCount._2))
     val textActualCounts = edgeLabelCounts.map(keyValue => 
       keyValue._1 + "\t" + keyValue._2.toString).reduce((k1, k2) => k1 +"\n" + k2)
     outputFileWriter.write("Actual edge counts\n")
     outputFileWriter.write(textActualCounts)
     outputFileWriter.write("\n\n\n")

     //Map edges 1 to 100 => 0, 100 to 200 => 1 and so on
     val dampedEdges = edgeLabelCounts.mapValues(v=> v/dampingFactor)
     val textDampedCounts = dampedEdges.map(keyValue => 
       keyValue._1 + "\t" + keyValue._2.toString).reduce((k1, k2) => k1 +"\n" + k2)
     println("Saving Damping Edge distribution by ", dampingFactor)
     outputFileWriter.write("Damepend edge counts by " + dampingFactor.toString + "\n")
     outputFileWriter.write(textDampedCounts + "\n")
     
     outputFileWriter.close()
     //dampedEdges.foreach(edgeLabelClass => println(edgeLabelClass._1, edgeLabelClass._2))
    /* val vectordampedEdges = Vector(dampedEdges.values)
     //val mean = breeze.stats.mean(dampedEdges.values)
     val minCount = edgeLabelCounts.values.min
     val maxCount = edgeLabelCounts.values.max
    //val mean = Math.  edgeLabelCounts.values
     val range = (maxCount-minCount)/numClasses
     val mappedEdges = edgeLabelCounts.mapValues(edgeLabelCount => (edgeLabelCount-minCount)/range).toMap
     println("Edge distribution classes with number of classes= ", numClasses)
     mappedEdges.foreach(edgeLabelClass => println(edgeLabelClass._1, edgeLabelClass._2))
     mappedEdges
     */
     dampedEdges.toMap
   }
     
}