package gov.pnnl.aristotle.algorithms.PathSearch

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.{SortedSet, Set}
import org.apache.spark.mllib.clustering._
import gov.pnnl.aristotle.utils.KGraphProp
import java.io._
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.Array.canBuildFrom
import gov.pnnl.aristotle.algorithms.PathSearch.PathSearchUtils._

object VertexFeatureGenerator {
  
  type EdgeWeight = Long
  type OntologyGraph = Graph[String, EdgeWeight]
  type NbrNode = (VertexId)
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("LASDemo").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("In ontology clustering")
    if(args.length != 4){
      println("check command line arguments")
      exit
    }
    val inputFile = args(0)
    val outputDir = args(1)
    val numClusters = args(2).toInt
    val numIter = args(3).toInt
    val g = ReadHugeGraph.getGraph(inputFile, sc)
    val vertexlabelFile =  outputDir + "/graphVertLabels"
   
   
    //groupEdgesByVertex(inputFile, sc)
    PrintWordnetOntologyStats(g, outputDir)
    PrintNodeToWordNetCategories(g, outputDir)
    //PrintWikiOntologyStats(g, outputDir)
  }
  
  
  /* Get number of yago entities connected to each wordnet category 
   * NOte1: each yago entity may connect to multiple wordnet categories
   * Note2: We are not counting the wikicategory_nodes connected to wordnet categories
   * (TODO: Check if we should enable wikicategory nodes as valid nodes )
   */
  def PrintWordnetOntologyStats(g: Graph[String, String], outputDir: String): Unit = {
    val wordnetEntities: VertexRDD[Int] = g.aggregateMessages[Int](triplet => {
      if( (!triplet.srcAttr.startsWith("wikicategory")) && 
          (triplet.dstAttr.startsWith("wordnet")) )
        triplet.sendToDst(1)
    }, (a,b) => a+b)
    
    val wordnetWithLabels = wordnetEntities.innerZipJoin(g.vertices)((id, numNbrs, label) => 
      (label, numNbrs))
    println("number of total wordnet categories=", wordnetWithLabels.count)
    wordnetWithLabels.saveAsTextFile(outputDir + "/wordnetClusterStats")
  }
  
  /* For each yago entity, including yago "wikicategory_" nodes, 
   * get theor wordnet category information 
   */
  def PrintNodeToWordNetCategories(g: Graph[String, String], outputDir: String): Unit = {
    val entitiesWithWordNetCatg : VertexRDD[Set[String]] = g.aggregateMessages[Set[String]](triplet => {
      if(triplet.dstAttr.startsWith("wordnet"))
        triplet.sendToSrc(Set(triplet.dstAttr))
    }, (a,b) => a ++ b)
    
    val entityLabelsWithWordNetCatg = entitiesWithWordNetCatg.innerZipJoin(g.vertices)(
        (id, wordnetCatg, label) => (label, wordnetCatg.fold[String]("")((a,b) => a + "," + b))) 
    println("number of total entities(regular nodes + wikicategory nodes) with some wordnet categories=", entityLabelsWithWordNetCatg.count)
    entityLabelsWithWordNetCatg.saveAsTextFile(outputDir + "/yagoEntitiesWordNetCatg")
  }
    
  /* For each WikiCategory get the data nodes that belong to that category */
  def PrintWikiOntologyStats(g: Graph[String, String], outputDir: String): Unit = {
  
    val wikiEntities: VertexRDD[Array[String]] = g.aggregateMessages[Array[String]](triplet => {
      if(triplet.attr == KGraphProp.edgeLabelNodeType && triplet.dstAttr.startsWith("wikicategory"))
        triplet.sendToDst(Array(triplet.srcAttr))
    }, (a,b) => a++b)
    
    val wikiWithLabels = wikiEntities.innerZipJoin(g.vertices)((id, nbrs, label) => (label, nbrs.size, nbrs))
    println("number of total wiki categories=", wikiWithLabels.count)
    val totalmemberSize = wikiWithLabels.map(_._2._2).sum
    println("number of total vertices with wikicategory available=", totalmemberSize)
    //wikiWithLabels.foreach(v => println(v._2._1, v._2._2, v._2._3.foreach(nbr => 
    //  print(nbr + " , ")) ))
    //wikiWithLabels.saveAsTextFile(outputDir + "/wikicategoryClusters")
  }
  
  def getFieldsFromLine(line :String) : Array[String] = {
    return line.toLowerCase().replaceAllLiterally("<", "").replaceAllLiterally(">", "").replace(" .", "").split("\\t").map(str => str.stripPrefix(" ").stripSuffix(" "));
  }
  
  
  /* Finds entity pairs with more than 1 edge between them. */
  def groupEdgesByVertexPair(inputFile: String, sc: SparkContext): Unit = {
    val triples = sc.textFile(inputFile).filter(ln => PathSearchUtils.isValidLine(ln)).map { line =>
        val fields = getFieldsFromLine(line);
        if (fields.length == 4)
          (fields(1), fields(2), fields(3))
        else if(fields.length == 3)
          (fields(0), fields(1), fields(2))
        else {
          println("Exception reading graph file line", line)
          ("None", "None", "None")
        }
      }

    triples.cache
    println("Number of triples =", triples.count)
    val edges: RDD[(String, String, String)] = triples.map(triple => {
      val src = triple._1
      val dst = triple._3
      if(src < dst){
        (src, triple._2, dst)
      }else 
        (dst, triple._2, src)
    }).distinct
    println("Number of unique triples =", edges.count)
    val edgeCountsBetweenEntities = edges.map(v => (v._1, v._3)).countByValue
    val multiEdgeEntities = edgeCountsBetweenEntities.filter(v => v._2 > 1)
    val multiEdges2 = multiEdgeEntities.filter(v => v._2 > 2)
    println("Number of vertex pairs with multiple edges > 1 ", multiEdgeEntities.size)
    println("Number of vertex pairs with multiple edges > 2 ", multiEdges2.size)
    multiEdges2.foreach(entityPairCount => 
        println(entityPairCount._1.toString, entityPairCount._2))
  }
 
}