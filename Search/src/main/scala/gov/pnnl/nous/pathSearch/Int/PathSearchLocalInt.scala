package gov.pnnl.nous.pathSearch.Int
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import java.io._
import gov.pnnl.nous.utils.ReadGraph
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import PathSearchIntDataTypes._



object PathSearchInt {
  
 
  def main(args: Array[String]): Unit = {
    
    if(args.length < 4){
      println("Usage <graphPath> <entityPairspath> <outDir> <maxIter>")
      System.exit(1)
    }
    
    val sparkConf = new SparkConf().setAppName("IntPathSearch").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val graphFile = args(0)
    val entityPairsFile = args(1)
    val outDir = args(2)
    val maxIter = args(3).toInt
    val maxDegree = -1
    run(graphFile, entityPairsFile, outDir, maxIter, sc, "\t", 3, maxDegree, 0.0)
  }
  
  def run(graphFile: String, entityPairsFile: String, outDir: String, maxIter: Int,
      sc: SparkContext, 
      sep: String, lineLen: Int,
      maxDegree: Int, topicCoherence: Double, 
      topicFile: String = "NONE", vertexTopicSep: String = "\t", topicSep: String = ",", 
      enableDestFilter: Boolean = false, enablePairFilter: Boolean = false): Unit = {
    
    val adjMap =  DataReader.getGraphInt(graphFile, sc, sep, lineLen).collect.toMap
    
    if(topicFile != "NONE") {
      println("Trying to read topics File", topicFile)
      val topics = DataReader.getTopics(topicFile, sc, vertexTopicSep, topicSep).collect.toMap
      val myFilter =new TopicFilter(topicCoherence)
      assert(topics.size == adjMap.size)
      FindPathsIntBatch(adjMap, entityPairsFile, sc, outDir, maxIter, myFilter, topics)
    } else if (maxDegree > 0){
      println("Trying to create degree filter with maxDegree", maxDegree)
      val myFilter = new DegreeFilter(maxDegree)
      val degree = adjMap.mapValues(_.size)
      FindPathsIntBatch(adjMap, entityPairsFile, sc, outDir, maxIter, myFilter, degree)  
    } else {
      println("Found no filter, executing regular path enumeration")
      val myFilter = new DummyFilter()
      FindPathsIntBatch(adjMap, entityPairsFile, sc, outDir, maxIter, myFilter, Map.empty)  
    }    
  }
  
  def FindPathsIntBatch[VD](adjMap: Map[VertexId, Iterable[IntEdge]], 
      entityPairsFile: String, sc: SparkContext, 
      outDir: String, maxIter: Int, pathFilter : PathFilter[VD], nodeFeatureMap: Map[VertexId, VD]): Unit = {
    
     val pairs: Array[(Int, Int)] = sc.textFile(entityPairsFile).map(ln => ln.split("\t"))
     .filter(_.length == 2)
     .map(arr => (arr(0).toInt, arr(1).toInt)).collect

     for (pair <- pairs) {
       try {
    	   val outFile: String = outDir + "/" + pair._1.toString + "__" + pair._2.toString
    	   val writer = new BufferedWriter(new FileWriter(outFile))
    	   val pathSofar: List[VertexId] = List(pair._1)
    	 
           val allPaths = FindPathsInt(pair._1, pair._2, adjMap, nodeFeatureMap,
               0, maxIter, pathFilter, pathSofar)
           println("Number of paths found between pairs", pair._1, pair._2, allPaths.length)
           for(path <- allPaths) {
             print(pair._1 + " : ")
             for(edge <- path) {
               print("(" + edge._2 + ") " + edge._1 + ", ")
               writer.write("(" + edge._2 + ") " + edge._1 + ", ")
             }
             println()
             writer.write("\n")
           }
    	   writer.flush()
           writer.close()
       } catch {
       case ioe :IOException => println("Could not open file")
       case e : Exception => println("COuld not execute serach on following pairs",pair._1, pair._2)
       }      
     }
  }
  
  def FindPathsInt[VD](src: VertexId, dest: VertexId, 
      adjMap: Map[VertexId , Iterable[IntEdge]], 
      nodeFeatureMap :  Map[VertexId , VD],
      currIter: Int, maxIter: Int, pathFilter : PathFilter[VD], 
      nodesSoFar: List[VertexId]): List[Path] = {
    
    var allPaths: List[Path] = List.empty
    if(src == dest || currIter >= maxIter)
      return allPaths
    
    val srcEdges : Iterable[IntEdge] = adjMap.get(src).get
    val directEdges: List[Path]  = srcEdges.filter(v => v._1 == dest).map(v => List(v)).toList
    allPaths = allPaths++directEdges
    
    val otherNbrEdges = srcEdges.filter(v => v._1 != dest)
    for(nbrEdge <- otherNbrEdges) {
      val nbrid: VertexId = nbrEdge._1
      if(nodeFeatureMap.size > 0) {
      val nbrData: VD = nodeFeatureMap.get(nbrid).get
      val prevNode: VertexId = nodesSoFar.last
      val prevNodeData: VD = nodeFeatureMap.get(prevNode).get
      if(!nodesSoFar.contains(nbrid) && 
          pathFilter.isValidNode(nbrData) && 
          pathFilter.isValidPair(prevNodeData, nbrData)){
        val newpath =  nodesSoFar.::(nbrid)
        val nbrPaths = FindPathsInt(nbrid, dest, adjMap, nodeFeatureMap,
               0, maxIter, pathFilter, newpath)
        val newPaths:  List[Path]  = nbrPaths.mapConserve(v => nbrEdge::v)
        allPaths = allPaths.++(newPaths)
      }
      } else {
        if(!nodesSoFar.contains(nbrid)) {
          val newpath =  nodesSoFar.::(nbrid)
          val nbrPaths:  List[Path]  = FindPathsInt(nbrid, dest, adjMap, nodeFeatureMap,
               0, maxIter, pathFilter, newpath)
          val newPaths:  List[Path]  = nbrPaths.mapConserve(v => nbrEdge::v)
          allPaths = allPaths.++(newPaths)
        }
      }
    }
    return allPaths    
  }
      
 
}

  