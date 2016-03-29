package gov.pnnl.aristotle.algorithms

import scala.io.Source
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import scala.collection.mutable.Set
import scala.collection._
import scala.collection.mutable.Map
import org.apache.spark.rdd.PairRDDFunctions

import gov.pnnl.aristotle.utils.TopicLearner
import scala.math.Ordering
import scala.util.Sorting
import java.io._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import gov.pnnl.aristotle.utils._

/** Basic data structure for edge. An Ordered Sequence of these constitutes a path
 *  
 */
class PathEdge(srcIdTemp: Long, srcLabelTemp:String, edgeLabelTemp: String, dstIdTemp: Long, 
    dstLabelTemp: String, isOutgoingTemp: Boolean = true) extends Serializable{
  var srcId = srcIdTemp
  var srcLabel = srcLabelTemp
  var isOutgoing = isOutgoingTemp
  var edgeLabel = edgeLabelTemp
  var dstId =  dstIdTemp
  var dstLabel = dstLabelTemp
  //var topicDiv = topicDest
  
  def edgeToString(): String = {
    var directionString = "->"
    if(isOutgoing == false) 
      directionString = "<-"
    val stringRep = srcId.toString + " : " + srcLabel + " : " + edgeLabel + " : " +  directionString +  
    " : "  + dstId.toString + " : " + dstLabel
    return stringRep
  }
  
  def containsId(vid : Long): Boolean = {
    return (srcId == vid || dstId == vid) 
  }
}

/** Basic trait To filter a PathEdge from inclusion in a path 
 *  
 */
trait NodeFilter[T] {
  def filter(value : T): Boolean 
}

/** Basic trait To define rank of an edge  and rank of a path
 *  Rank of a path = SomeFunction(of all edge in path)
 *  
 */

trait PathRank {
  def edgeRank(edge: PathEdge): Double
  def pathRank(path: List[PathEdge]): Double
}



class TopicFilter(destTopicTemp : Array[Double], matchThresholdTemp : Double) extends NodeFilter[(String, Array[Double])] {
  val destTopic: Array[Double] = destTopicTemp
  val matchThreshold : Double = matchThresholdTemp
  
  def filter(targetNode: (String, Array[Double]) ): Boolean = {
    return (Math_Utils.jensenShannonDiv(targetNode._2, destTopic) > matchThreshold)
  }
}

class DegreeFilter(maxDegreeTemp: Long, vertexDegreeTemp : RDD[(Long, Int)]) extends NodeFilter[PathEdge] {
  val maxDegree = maxDegreeTemp
  val vertexDegree = vertexDegreeTemp
  
  def filter(edge: PathEdge): Boolean = {
    return ((vertexDegree.lookup(edge.srcId)(0) > maxDegree) || (vertexDegree.lookup(edge.dstId)(0) > maxDegree))     
  }
}


/*
object PathProp {
  def getNbrEdges(g: Graph[(String, Array[Double]), String], filterRelations : Array[String]= Array.empty): VertexRDD[Set[PathEdge]] ={
    
    val nbrEdges = g.aggregateMessages[Set[PathEdge]](edge =>{
      if(!filterRelations.contains(edge.attr))
      //edge.sendToSrc(Set(new PathEdge(edge.dstId, edge.dstAttr._1, true , edge.attr, Math_Utils.sqDistance(edge.srcAttr._2, edge.dstAttr._2))))
      //edge.sendToDst(Set(new PathEdge(edge.srcId, edge.srcAttr._1, false, edge.attr, Math_Utils.sqDistance(edge.dstAttr._2, edge.srcAttr._2))))
      edge.sendToSrc(Set(new PathEdge(edge.dstId, edge.dstAttr._1, true , edge.attr, edge.dstAttr._2)))
      edge.sendToDst(Set(new PathEdge(edge.srcId, edge.srcAttr._1, false, edge.attr, edge.dstAttr._2)))
    }, 
    (a,b) => a ++ b
    ) 
    return nbrEdges
  }
}
*/
class IntentType(answerTypeTemp:String="", constraintTypeTemp:String ="", constraintValueTemp :String="") extends Serializable{
  
  val answerType: String = answerTypeTemp
  val constraintType : String = constraintTypeTemp
  val constraintValue: String = constraintValueTemp
}



object PathRankingUsingTopics {
   def isValidLine(ln : String) : Boolean ={
    ( (ln.startsWith("@") ==false) && (ln.startsWith("#")==false) && (ln.isEmpty()==false))
   }
  
   def main(args: Array[String]): Unit = {     
    val sparkConf = new SparkConf().setAppName("get yago topics").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\fake_hadoop\\")
    println("starting from main")
    if(args.length < 4) {
      println("Usage <Graph Object Path(containing vertices and edges directory)> " + 
          "<Entity-> TopicDistribution File(FULL)> <File with Entity Pairs for Finding Path<entity1,entity2>" +
          "<maxpathSize>  <IgnoreNodesWithDegree(Optional)>")
      println("Outputs file ($EntityPairFilePath.pathRanks.out)")
    }
    
    var nodeDegreeThreshold  = Int.MaxValue
    if(args.length == 5){
      nodeDegreeThreshold = args(4).toInt
    }
    
    //RankPathsUsingTopics(args(0), args(1), args(2), args(3).toInt, sc, nodeDegreeThreshold) 
    val graphFile = args(0)
    val src = args(1)
    val dest = args(2)
    val numIteration = args(3).toInt
    val topicsFile = args(4)
    TestPregelPathsWithTopic(graphFile, src, dest, numIteration , topicsFile, sc)
    //println("number of paths found = ", paths.length)
  }
  
  def TestPregelPathsWithTopic(graphFile: String, srcLabel: String, destLabel: String, maxPathSize :Int, 
      topicsFile: String, sc: SparkContext): Unit = {
    if(srcLabel == destLabel) {
      println("Please provide two different entities")
      return
    } 
   
    val g: Graph[ String, String] = ReadHugeGraph.getGraph(graphFile, sc)
    
    val srcCandidate = g.vertices.filter(v => v._2 == srcLabel.toLowerCase()).collect.apply(0)
    val destCandidate = g.vertices.filter(v => v._2 == destLabel.toLowerCase()).collect.apply(0)
     
  
    val pathSrcId = srcCandidate._1
    val pathDstId = destCandidate._1
    
    println(" Adding topics to graph")
    val graphWithTopicsOnVertices: Graph[nodeWithAllTopics[String], String] = TopicLearner.addTopicsToGraphAll(g, topicsFile, sc).cache
    println("Done adding topics to graph. sample vertex=", graphWithTopicsOnVertices.vertices.first)
    g.unpersist(false)
    
    val entities : Array[(VertexId, nodeWithAllTopics[String])] = MapLabelToIds(srcLabel, destLabel, 
          graphWithTopicsOnVertices)
    println("src =", entities(0)._1, entities(0)._2.data)
    println("dest =", entities(1)._1, entities(1)._2.data)
   
    val topicFilter = new TopicFilter(entities(1)._2.topic, 0.001)
    
    //val allPaths = runPregel(entities(0), entities(1), graphWithTopicsOnVertices, topicFilter, maxPathSize)
  }

  def runPregel[T](src: (VertexId, nodeWithAllTopics[String]), dest: (VertexId, nodeWithAllTopics[String]), 
      g: Graph[nodeWithAllTopics[String], String], 
     filterObj: NodeFilter[T], numIter: Int): List[List[PathEdge]] = {
     
     val pathSrcId = src._1
     val pathDstId = dest._1
     
     val initialMsg = List.empty[List[PathEdge]]
     val activeDirection = EdgeDirection.Both
     val pregelGraph = g.mapVertices((id, nodeData) => (nodeData, List.empty[List[PathEdge]])).cache
     
     val messages = pregelGraph.pregel[List[List[PathEdge]]](initialMsg, numIter, activeDirection)(
       //Pregel Vertex program
       (vid, labelWithTopic, newPathsReceived) => { 
         // On Vertex , if you have reached destination, save all the paths traversed so far
         //first filter paths that were sent by this receiving vertex
         println("id =" + vid.toString + ", paths in previous iteration =", labelWithTopic._2.length, labelWithTopic._2.foreach(v => v.toString))
         println("id = " + vid.toString + " new paths = "  + newPathsReceived.length)
         newPathsReceived.foreach(path => path.foreach(v=> println(v.edgeToString)))
         
         val cleanedupPathsFromPrevIter =  newPathsReceived //.filter(path => path.exists(edge => edge.containsId(vid)))
         val newVertexData =
         if(vid==pathDstId) 
           (labelWithTopic._1, labelWithTopic._2 ++ cleanedupPathsFromPrevIter)
         else
           //if not destination vertex, discard paths from previous iteration and work with results from this iteration
           (labelWithTopic._1, cleanedupPathsFromPrevIter)
          
           newVertexData
        },
       
       //Pregel Send Message 
       triplet => {
         
         val receivedPathsSrc = triplet.srcAttr._2
         val receivedPathsDest = triplet.dstAttr._2
         val iteration = getIterationId(receivedPathsSrc, receivedPathsDest)
         println("In Send Message for triplet, iteration = ", triplet.srcId, triplet.dstId, iteration)
         
         //if its Start of iteration (=0)
         if(iteration == 0) {
           // Either edge.sourceNode should be Path_starter_node
           if(triplet.srcId == pathSrcId){
             println("I am path source and edge source, sending data to tripletdestination ", triplet.srcId, triplet.dstId)         
             val path =  new PathEdge(triplet.srcId, triplet.srcAttr._1.data,  triplet.attr, triplet.dstId, triplet.dstAttr._1.data, true)
             Iterator((triplet.dstId, List(List(path))))
           } else if (triplet.dstId == pathSrcId){
             // Or edge.destNode should be Path_starter_node
             println("I am path source and edge destination, sending data to triplet.source ", triplet.dstId, triplet.srcId)
             val path =  new PathEdge(triplet.dstId, triplet.dstAttr._1.data,  triplet.attr, triplet.srcId, triplet.srcAttr._1.data, false)
             Iterator((triplet.srcId, List(List(path))))
           } else {
             Iterator.empty
           }
         } 
          
         //All other iterations, 
         //A triplet is active, iff source and/or destination have received a message from previous iteration
          else { 
            var sendMsgIterator: Set[(VertexId, List[List[PathEdge]])] = Set.empty
           
            if(isNodeActive(triplet.srcId, receivedPathsSrc, iteration, pathDstId)) {              
              println("Chceking if this edge is a good candidate")
              //val isEdgeGood = filterObj.filter((triplet.dstAttr._1.data, triplet.dstAttr._1.topic))
              
              println(" Triplet.source" + triplet.srcId + ",is active, checking for non-cyclic candidate paths")
              receivedPathsSrc.foreach(path => path.foreach(edge => println(edge.edgeToString)))             
              
              val filteredPathsSrc = receivedPathsSrc.filter(path => path.exists(edge => !edge.containsId(triplet.dstId)))
              if(filteredPathsSrc.length != 0) {
                println("Valid Paths( without possible cycles =" + receivedPathsSrc.length)
                val newEdgeToAddToPathsSrc = new PathEdge(triplet.srcId, triplet.srcAttr._1.data, triplet.attr, 
                    triplet.dstId, triplet.dstAttr._1.data, true) 
                //Append new edge to remaining and send
                val newPathsSrc = filteredPathsSrc.map(path => newEdgeToAddToPathsSrc :: path)
                newPathsSrc.foreach(path => path.foreach(edge => println(edge.edgeToString)))
                val sendMsgDest = (triplet.dstId,  newPathsSrc)
                sendMsgIterator = sendMsgIterator.+(sendMsgDest)
              }
            } 
            
            if(isNodeActive(triplet.dstId, receivedPathsDest, iteration, pathDstId)) {              
              println(" Triplet.dest" + triplet.dstId + ", received data in previious iteration, prepring data to send to ", triplet.srcId)
              receivedPathsDest.foreach(path => path.foreach(edge => println(edge.edgeToString)))
              
              val filteredPathsDest = receivedPathsDest.filter(path => path.exists(edge => !edge.containsId(triplet.srcId)))
              if(filteredPathsDest.length != 0) {
                println("Valid Paths( without possible cycles =" + receivedPathsDest.length)
                val newEdgeToAddToPathsDest = new PathEdge(triplet.dstId, triplet.dstAttr._1.data, 
                    triplet.attr, triplet.srcId, triplet.srcAttr._1.data, false)
              
                //Append new edge to remaining and send
                val newPathsDst = filteredPathsDest.map(path => newEdgeToAddToPathsDest :: path)
                println("vertex id , sending", triplet.dstId)
                newPathsDst.foreach(path => path.foreach(edge => println(edge.edgeToString)))
                val sendMsgSrc = (triplet.srcId,  newPathsDst)
                sendMsgIterator = sendMsgIterator.+(sendMsgSrc)
              }
            }
            
            println("Length of sendMsgIterator =", sendMsgIterator.size)
            sendMsgIterator.toIterator
          }
       },
       //Pregel Merge message
       (pathList1, pathList2) => pathList1 ++ pathList2
       )
       val allPathsToDestination = messages.vertices.filter(_._1 == pathDstId).collect.apply(0)._2._2
       
       println("FINAL paths to destination")
       allPathsToDestination.foreach(path => {
         println("Path =" )
         path.foreach(edge => println(edge.edgeToString))
         })
         
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
       // checking if data is valid for thsi iteration
       return (pathList.head.size == iteration)          
     }
   }
   
  /* 
   def RankPathsUsingTopics(graphPath: String, topicsFile: String, entityPairsFile: String, maxPathSize :Int,
        sc: SparkContext, ignoreNodesWithHighDegree:Int =Int.MaxValue) : Unit = {
     
     val vertexFile = graphPath + "/vertices"
     val edgeFile = graphPath + "/edges"
     
     println("Reading graph")
     val graph = ReadHugeGraph.getGraphObj(vertexFile,  edgeFile, sc)
     println("Done reading graph, first vertex =", graph.vertices.first)
     
     println(" Adding topics to graph")
     val graphWithTopicsOnVertices: Graph[nodeWithAllTopics[String], String] = TopicLearner.addTopicsToGraphAll(graph, topicsFile, sc).cache
     println("Done reading graph, first vertex =", graphWithTopicsOnVertices.vertices.first)
 /*    
    // Get a vertexRDD containing adjacency list
     val listNbrEdges: VertexRDD[Set[PathEdge]] = PathProp.getNbrEdges(graphWithTopicsOnVertices,  ignoreRelations)
     val listNbrEdgesWithMyData  = graphWithTopicsOnVertices.vertices.leftZipJoin(listNbrEdges) {
       case(vid, nodeData, Some(edgeList)) => (nodeData, edgeList)
       case(vid, nodeData, None) => (nodeData, Set.empty)
     }
   */  
    
     /* NOTE: INTENT is to enable filters on type of answers that we return, like type of nodes in the path, degree of nodes, type of edges etc 
     val intent = new IntentType()
     //Question1: Places to visit with children in Orlando 
     // INtent: find path between"Orlando" and "kids" and return all nodes 
     //that are of type "location" and have strongly related to "Children/Youth" topics, 
     // but we have no way to say which topic in our distribution 
     //is related to Children ?
     val intentOrlandoKids : List[IntentType] = List(new IntentType("entity", "rdf:type", "location"), 
     new IntentType("path", "topicFilter", "Children"))
    
     //Question: Why GoPro needs to develop drones, 
     // Intent: include all paths, including data that is not reliably hand marked
     // but avoid generic answers
     val intentDrones: List[IntentType] = List(new IntentType("path", "vertexFilter", "#nbrs <= 1000"))
     */
     // can add more or create a degree based node elimination 
     val ignoreRelations: Array[String] = Array("hasGender")
     val listEntityLabelPairs: Iterator[(String, String)] = GetEntityPairs(entityPairsFile)
     
     for(entityLabelPair <- listEntityLabelPairs){
       
      val entities : Array[(VertexId, nodeWithAllTopics[String])] = MapLabelToIds(entityLabelPair._1, entityLabelPair._2, 
          graphWithTopicsOnVertices)
      val allPaths: List[List[PathEdge]] = FindRankedPaths(entities(0), entities(1) ,graphWithTopicsOnVertices, 
          5, sc, ignoreRelations, ignoreNodesWithHighDegree)
      var result: String =""
  /*    for (paths <- allPaths) {
        for(edge <- paths) {
          result += "<" + edge.srcLabel + ">---" + edge.edgeLabel + " (" + edge.topicDiv +") ---> <" + edge.destLabel + "> ; "
        }
      } 
      */
      Gen_Utils.writeToFile(entityPairsFile +"." + entities(0)._2 + "." + entities(1)._2 + ".out", result)
     }
     
   }
   
   class pregelGraphNode[T](nodeDataTemp: T, pathTemp: String) {
     val nodeData : T = nodeDataTemp
     val path = pathTemp
   }
 
 
      /* given 2 node ids, finds all 0-4 hop path b/w them */
  
  def FindRankedPaths(src: (VertexId, nodeWithAllTopics[String]), dest: (VertexId, nodeWithAllTopics[String]), 
      g: Graph[nodeWithAllTopics[String], String], maxPathSize: Int, sc: SparkContext, 
      filterRelations: Array[String] = Array.empty, filterNodesWithDeg:Int = Int.MaxValue): List[List[PathEdge]] ={

   val pathSrcId: VertexId = src._1; val srcLabel: String = src._2.data; val srcTopicDist = src._2.topic
   val pathDstId: VertexId = dest._1; val dstLabel: String = dest._2.data; val destTopicDist = dest._2.topic
   val destTopicBroadcast = sc.broadcast(destTopicDist)
   val destTopicValue = destTopicBroadcast.value
   val topicMatchThreshold = 0.1
   
   val initialMsg = List.empty[List[PathEdge]]
   val activeDirection = EdgeDirection.Both
   val pregelGraph = g.mapVertices((id, nodeData) => (nodeData, List.empty[List[PathEdge]])).cache
   //To avoid cycles, check if the receiving vertex is already part of the path, then don't send the msg
   // A vertex can't restrict pregel to only consider topK out messages at each node, but we can write our own pregel for that
   val messages = pregelGraph.pregel[List[List[PathEdge]]](initialMsg, maxPathSize, activeDirection)(
       //Pregel Vertex program
       (vid, labelWithTopic, newPathsReceived) => { 
         // On Vertex , if you have reached destination, save all the paths traversed so far
         //first filter paths that were sent by this receiving vertex
         val cleanedupPathsFromPrevIter =  newPathsReceived.filter(path => path.exists(edge => edge.containsId(vid)))
         if(vid==pathDstId) 
           (labelWithTopic._1, labelWithTopic._2 ++ cleanedupPathsFromPrevIter)
         else
           //if not destination vertex, discard paths from previous iteration and work with results from this iteration
           (labelWithTopic._1, cleanedupPathsFromPrevIter)
       },
       
       //Pregel Send Message 
       triplet => {
         //if its Start of iteration=1 and edge.sourceNode = Path_starter_node
          if(triplet.srcId == pathSrcId && triplet.srcAttr._2.length == 0){
            val path =  new PathEdge(triplet.srcId, triplet.srcAttr._1.data,  triplet.attr, triplet.dstId, triplet.dstAttr._1.data, true)
            Iterator((triplet.dstId, List(List(path))))
          }
          //if its Start of iteration=1 and edge.destNode = Path_starter_node
          else if (triplet.dstId == pathSrcId && triplet.dstAttr._2.length == 0) {
            val path =  new PathEdge(triplet.dstId, triplet.dstAttr._1.data,  triplet.attr, triplet.srcId, triplet.srcAttr._1.data, false)
            Iterator((triplet.srcId, List(List(path))))
          } 
          //All other iterations, if I have received a message form previous iteration
          else { 
            val receivedPaths = triplet.srcAttr._2              
            val isOutgoingTriplet = isOutgoingEdge(receivedPaths.last.last, triplet.srcId, triplet.dstId)          
            // Figure out the edge destination and its direction
            val newEdgeToAddToPaths = 
              if(isOutgoingTriplet) {
                new PathEdge(triplet.srcId, triplet.srcAttr._1.data, triplet.attr, triplet.dstId, triplet.dstAttr._1.data, true)
              } else {
                new PathEdge(triplet.dstId, triplet.dstAttr._1.data, triplet.attr, triplet.srcId, triplet.srcAttr._1.data, false)
              }
             
            if(newEdgeToAddToPaths.srcId == pathDstId){
              //If reached destination           
              Iterator.empty
            } else {
              //Go to through list of paths you received in previous iteration and filter potential cycles
              receivedPaths.filter(path => path.exists(edge => edge.containsId(newEdgeToAddToPaths.dstId)))
              //Append new edge to remaining and send
              receivedPaths.foreach(path => path.::(newEdgeToAddToPaths))

              Iterator((newEdgeToAddToPaths.dstId, receivedPaths))
            }
          }
       },
       //Pregel Merge message
       (pathList1, pathList2) => pathList1 ++ pathList2
       )
       val allPathsToDestination = messages.vertices.filter(_._1 == pathDstId).collect.apply(0)._2._2
       return allPathsToDestination
  }
  
*/
  
   def MapLabelToIds(labelSrc: String, labelDest: String, g : Graph[nodeWithAllTopics[String], String] ): 
   Array[(VertexId, nodeWithAllTopics[String])] = {   
    val candidates : VertexRDD[nodeWithAllTopics[String]] = g.vertices.filter(v => v._2.data.toLowerCase().startsWith(labelSrc.toLowerCase()) 
        || v._2.data.toLowerCase().startsWith(labelDest.toLowerCase()))
    
    var srcCandidates: Array[(VertexId, nodeWithAllTopics[String])] = candidates.filter(v => v._2.data.toLowerCase().startsWith(labelSrc.toLowerCase())).collect
    var destCandidates: Array[(VertexId, nodeWithAllTopics[String])] = candidates.filter(v => v._2.data.toLowerCase().startsWith(labelDest.toLowerCase())).collect
    if(srcCandidates.length < 1 || destCandidates.length < 1){
      println("No Match found for the provided labels #(src, dest) matches", srcCandidates.length, destCandidates.length)
      return Array.empty[(VertexId, nodeWithAllTopics[String])]
    }
    
    Sorting.quickSort(srcCandidates)(Ordering.by[(VertexId, nodeWithAllTopics[String]), String](_._2.data))
    Sorting.quickSort(destCandidates)(Ordering.by[(VertexId, nodeWithAllTopics[String]), String](_._2.data))
    // NOte we can use EntityDisambiguation module here if needed
    println("Choosing first one, among available matches for " + labelSrc + " = ")
    srcCandidates.foreach(v => println(v._2)) //(v = > println(v._2))
    println("Choosing first one, among available matches for " + labelDest + " = ")
    destCandidates.foreach(v => println(v._2))
    
    return Array(srcCandidates(0), destCandidates(0))
 
   }
   
  
   def GetEntityPairs(filename: String): Iterator[(String, String)] = {
     Source.fromFile(filename).getLines().filter(isValidLine(_)).map(v=> v.split("\t")).filter(_.size == 2).map(v => (v(0), v(1)))
   }

}