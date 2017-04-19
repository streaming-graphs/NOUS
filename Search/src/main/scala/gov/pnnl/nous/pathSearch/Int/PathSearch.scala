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



object PathSearch {
  
 
  def main(args: Array[String]): Unit = {
    
    if(args.length < 5){
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
    val lineLen  = args(4).toInt
    val maxDegree = -1
    
    run(graphFile, entityPairsFile, outDir, maxIter, sc, "\t", lineLen, maxDegree, 0.0)
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
     val psObj = new PathSearchRec(adjMap, nodeFeatureMap, pathFilter, maxIter)
     for (pair <- pairs) {
       try {
    	   val outFile: String = outDir + "/" + pair._1.toString + "__" + pair._2.toString
    	   val writer = new BufferedWriter(new FileWriter(outFile))
    	   val pathSofar: List[VertexId] = List(pair._1)
    	 
           val allPaths = psObj.FindPathsInt(pair._1, pair._2, 0, pathSofar)
           println("Number of paths found between pairs", pair._1, pair._2, allPaths.length)
           for(path <- allPaths) {
             val srcString = pair._1 + " : "
             print(srcString)
             writer.write(srcString)
             for(edge <- path) {
               val direction : String = if(edge._3) "Out" else "In"
               val edgeLabel : String = if(edge._2 == defaultEdgeLabel) "" else edge._2.toString + "-"
               val line = "(" + edgeLabel + direction + ") " + edge._1 + ", "
               print(line)
               writer.write(line)
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
  
  
 
}

  