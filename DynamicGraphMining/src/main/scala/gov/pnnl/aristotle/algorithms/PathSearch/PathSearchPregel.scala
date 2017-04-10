package gov.pnnl.aristotle.algorithms.PathSearch

import scala.io.Source
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import scala.math.Ordering
import scala.util.Sorting
import java.io._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import gov.pnnl.aristotle.utils._
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import org.apache.spark.graphx.Graph.graphToGraphOps

object PathSearchPregel {

   def main(args: Array[String]): Unit = {     
    val sparkConf = new SparkConf().setAppName("get all paths")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
   
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\fake_hadoop\\")
    println("starting from main")
    if(args.length < 5) {
      println("Usage <Graph Path> " +
          "<File with Entity Pairs for Finding Path<entity1,entity2> " + 
          "<outputDir> " +
          "<maxpathSize> <degreeFilter>")
      exit
    }
    
    val graphFile = args(0)
    val entityPairsFile = args(1)
    val outputDir = args(2) 
    val numIteration = args(3).toInt
    val t0 = System.nanoTime()    
    FindPathsUsingPregelBatch(graphFile, entityPairsFile , numIteration, 
        outputDir, sc, args.drop(4))
    val t1= System.nanoTime()
    println("Total Execution Time(s)=", (t1-t0)/1000000000L)
   }
 
  
  /* 1) Reads Graph, 
   * 2) Maps labels to  graph entity (src, dest) pairs and 
   * 3) create filter function objects
   * 3) use pregel to find paths b/w entity pairs in the graph
   * 4) Write Paths to File
   */ 
  def FindPathsUsingPregelBatch(graphFile: String, entityPairsFile: String, 
      maxPathSize :Int, outputDir: String,  sc: SparkContext, filterFuncArgs: Array[String]): Unit = {
       
    //val filterType = getFilterType(filterFuncArgs) 

    val listEntityLabelPairs: List[(String, String)] = PathSearchUtils.GetEntityPairs(entityPairsFile, sc)  
    println("Entity pairs = ", listEntityLabelPairs.size) 
    //listEntityLabelPairs.foreach(v => println(v._1 + ", " +  v._2)) 
    if(listEntityLabelPairs.size == 0){
      println("Could not read any valid entity pairs")
      exit
    }

    if(filterFuncArgs.length != 1){
      println("Noargs specified for vertexdegree filter")
      println(" provide max degree")
      //println("Provide <TopicFile> <TopicMatchThreshold> in addition to usual args")
      exit
    }
    
    val ts_read = System.nanoTime()
    val g: Graph[String, String] = ReadHugeGraph.getGraph(graphFile, sc)
    val te_read = System.nanoTime()
   
    println("Graph Read Time(s)=", (te_read-ts_read)/1000000000L)
    println("Adding vertex degree to graph")
    val vertexDegreeRDD = g.degrees 
    val maxDegree = filterFuncArgs(0).toInt
    val gExtended = g.outerJoinVertices(vertexDegreeRDD)((id, value, degree) => new ExtendedVD(value, degree))
    val filterObj = new MaxDegreeFilter(maxDegree)
    
    /*
    val topicsFile = filterFuncArgs(0)
    println("Adding topic to graph")
    val gExtended = AddTopicsToGraph.addAll(g, topicsFile, sc)
    val topicMatchThreshold = filterFuncArgs(1).toDouble
   */
    println("Done adding data to graph. sample vertex=", gExtended.vertices.first)
    val te_join = System.nanoTime()
    println("#Graph Feature join Time(s)=", (te_join- te_read)/1000000000L)
    gExtended.cache
   
    for(entityLabelPair <- listEntityLabelPairs){
      val srcLabel = entityLabelPair._1
      val destLabel = entityLabelPair._2
      if(srcLabel == destLabel) {
        println("Please provide two different entities, skipping this pair", srcLabel, destLabel)
      } else {
        val tstart_search = System.nanoTime()
        val entities  = PathSearchUtils.getBestStringMatch(srcLabel, destLabel, gExtended)
        if(entities.length == 2) {
          val src = entities(0)
          val dest = entities(1)
          println("srcId, srcLabel =", src._1, src._2.label)
          println("destId, destLabel =", dest._1, dest._2.label)
          
          //val filterObj = new TopicFilter(dest._2.extension.getOrElse(Array.empty[Double]), topicMatchThreshold)
          val tstart_walk = System.nanoTime()
          val allPaths = runPregel(src, dest, gExtended, filterObj, maxPathSize, EdgeDirection.Either)
          val tend_walk = System.nanoTime()
          println("Number of paths = " + allPaths.length)
          println("Graph Walk Time (s)=", (tend_walk-tstart_walk)/1000000000L)
          
          val outFile = outputDir  + "/" + srcLabel + "." + destLabel + ".paths.out" 
          PathSearchUtils.writePaths(allPaths, outFile)
        }
        val tend_search = System.nanoTime()
        println("Graph Search Total Time(Entity Disamb + Walk)(s)=", (tend_search- tstart_search)/1000000000L)
      }
    }
    
  }

  /*Walk Graph "g" : starting from "Src" and ending at "dest" , where maximum path length = "numIter"
   * Ignore any node which does not pass the "nodeFilter" test
   * The Walk is done considering edges in either direction 
   * */
  def runPregel[VD, VD2](src: (VertexId, ExtendedVD[VD, VD2]), dest: (VertexId, ExtendedVD[VD, VD2]), 
      g: Graph[ExtendedVD[VD, VD2], String], nodeFilter: NodeFilter[ExtendedVD[VD, VD2]], numIter: Int, activeDirection : EdgeDirection): List[List[PathEdge]] = {
     
     val pathSrcId = src._1
     val pathDstId = dest._1    
     val initialMsg = List.empty[List[PathEdge]]    
     val pregelGraph = g.mapVertices((id, nodeData) => (nodeData, List.empty[List[PathEdge]])).cache
     
     val messages = pregelGraph.pregel[List[List[PathEdge]]](initialMsg, numIter, activeDirection)(
       
       //Pregel Vertex program
       (vid, vertexDataWithPaths, newPathsReceived) => { 
         // If reached destination, save all the paths received so far, 
         // else, discard paths from previous iteration and work with results from this iteration
           if(vid==pathDstId) 
             (vertexDataWithPaths._1, vertexDataWithPaths._2 ++ newPathsReceived)
           else
             (vertexDataWithPaths._1, newPathsReceived)      
        },
       
       //Pregel Send Message 
       triplet => {
         
         val receivedPathsSrc = triplet.srcAttr._2
         val receivedPathsDest = triplet.dstAttr._2
         val iteration = getIterationId(receivedPathsSrc, receivedPathsDest)
         
         //if its Start of iteration (=0)
         if(iteration == 0) {
           // Either edge.sourceNode should be Path_starter_node
           if(triplet.srcId == pathSrcId && (nodeFilter.isMatch(triplet.dstAttr._1) || triplet.dstId == pathDstId)){        
             val path =  new PathEdge(triplet.srcId, triplet.srcAttr._1.labelToString ,  triplet.attr, triplet.dstId, triplet.dstAttr._1.labelToString , true)
             Iterator((triplet.dstId, List(List(path))))
           } else if (triplet.dstId == pathSrcId && (nodeFilter.isMatch(triplet.srcAttr._1) || triplet.srcId == pathDstId)){
             // Or edge.destNode should be Path_starter_node
             val path =  new PathEdge(triplet.dstId, triplet.dstAttr._1.labelToString,  triplet.attr, triplet.srcId, triplet.srcAttr._1.labelToString, false)
             Iterator((triplet.srcId, List(List(path))))
           } else {
             Iterator.empty
           } 
         } 
          
         //All other iterations: A triplet is active, 
         // iff source and/or destination have received a message from previous iteration
          else { 
            var sendMsgIterator: Set[(VertexId, List[List[PathEdge]])] = Set.empty
           
            // Is triplet.source an active vertex
            if(isNodeActive(triplet.srcId, receivedPathsSrc, iteration, pathDstId)  && 
                (nodeFilter.isMatch(triplet.dstAttr._1) || triplet.dstId == pathDstId)) {
              
              val filteredPathsSrc = receivedPathsSrc.filter(path => path.exists(edge => !edge.containsId(triplet.dstId)))
              if(filteredPathsSrc.length != 0) {
               // println("Valid Paths( without possible cycles =" + filteredPathsSrc.length)
                val newEdgeToAddToPathsSrc = new PathEdge(triplet.srcId, triplet.srcAttr._1.labelToString, triplet.attr, 
                    triplet.dstId, triplet.dstAttr._1.labelToString, true) 
                //Append new edge to remaining and send
                val newPathsSrc = filteredPathsSrc.map(path => newEdgeToAddToPathsSrc :: path)
                val sendMsgDest = (triplet.dstId,  newPathsSrc)
                sendMsgIterator = sendMsgIterator.+(sendMsgDest)
              }
            } 
            
            // Is triplet.destination an active vertex
            if(isNodeActive(triplet.dstId, receivedPathsDest, iteration, pathDstId) && 
                (nodeFilter.isMatch(triplet.srcAttr._1) ||  triplet.srcId == pathDstId)) {              
            
              val filteredPathsDest = receivedPathsDest.filter(path => path.exists(edge => !edge.containsId(triplet.srcId)))
              if(filteredPathsDest.length != 0 ) {
                 // println("Valid Paths( without possible cycles =" + filteredPathsDest.length)
                val newEdgeToAddToPathsDest = new PathEdge(triplet.dstId, triplet.dstAttr._1.labelToString, 
                    triplet.attr, triplet.srcId, triplet.srcAttr._1.labelToString, false)
              
                //Append new edge to remaining and send
                val newPathsDst = filteredPathsDest.map(path => newEdgeToAddToPathsDest :: path)
                val sendMsgSrc = (triplet.srcId,  newPathsDst)
                sendMsgIterator = sendMsgIterator.+(sendMsgSrc)
              }
            }
            sendMsgIterator.toIterator
          }
       },
       
       //Pregel Merge message
       (pathList1, pathList2) => pathList1 ++ pathList2
     ) // End of Pregel
       
     val allPathsToDestination = messages.vertices.filter(_._1 == pathDstId).collect.apply(0)._2._2
     println("FINAL PATHS TO DESTINATION", allPathsToDestination.length)         
     return allPathsToDestination  
   }


 
   def getIterationId(pathList1:  List[List[PathEdge]], pathList2: List[List[PathEdge]] ): Int = {
    
     if(pathList1.length == 0 && pathList2.length == 0)
       return 0
     else {
       val numPaths1 = pathList1.length
       val numPaths2 = pathList2.length
       if(numPaths1 == 0) {
         return pathList2.head.size
       }
       else if(numPaths2 == 0) {
         return pathList1.head.size
       } else {
       // Both lists have data
         return Math.max(pathList1.last.size, pathList2.last.size)
       }
     } 
     
   }
 
   def isNodeActive(nodeId: Long, pathList : List[List[PathEdge]], iteration: Int, finalDestId: Long ): Boolean = {
     if(nodeId == finalDestId || pathList.length == 0) {
       return false 
     } else {
       //=> Node is not destination and has some data, 
       // checking if data is valid for this iteration
       return (pathList.head.size == iteration)          
     }
   }
  
}