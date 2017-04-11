package gov.pnnl.nous.pathSearch
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import gov.pnnl.nous.utils.MathUtils




/** Basic data structure for edge. An Ordered Sequence of these constitutes a path
 *  
 */
class PathEdge(val srcId: Long, val srcLabel:String, val edgeLabel: String, val dstId: Long, 
   val dstLabel: String, val isOutgoing: Boolean = true) extends Serializable{
  
  def edgeToString(): String = {
    var directionString = "Outgoing"
    if(isOutgoing == false) 
      directionString = "Incoming"
    val stringRep = directionString + " ; " + srcLabel + " ; " + 
    edgeLabel + " ; "  + dstLabel
    return stringRep
  }
  
  def containsId(vid : Long): Boolean = {
    return (srcId == vid || dstId == vid) 
  }
}


/** Class template to extends graph vertex data with auxiliary data[Optional]
 *  Auxiliary data is used to prune edges from a path during graph walk
 *  
 */
class ExtendedVD[VD, VD2] (val label: VD,val extension : Option[VD2]) extends Serializable  {
  def labelToString(): String = {
    label.toString
  }
  
  def extToString(): String = {
   extension match {
     case Some(extension) => extension.toString
     case _ => ""
   }
  }
}


/** Basic trait To filter a PathEdge from inclusion in a path 
 *  
 */
class NodeFilter[T] extends Serializable{
  def isMatch(value : T): Boolean = {
    true
  }
}


/* Implements topic filter using jensenShnnaon divergence */
class TopicFilter(destTopicTemp : Array[Double], matchThresholdTemp : Double) extends NodeFilter[ExtendedVD[String, Array[Double]]] {
  val destTopic: Array[Double] = destTopicTemp
  val matchThreshold : Double = matchThresholdTemp
  
  override def isMatch(targetNode: ExtendedVD[String, Array[Double]] ): Boolean = {
    val isMatch: Boolean = 
      targetNode.extension match {
      case Some(topic) => MathUtils.jensenShannonDiv(topic, destTopic) < matchThreshold
      case None   => true
    }
    isMatch
  }
}


/* Implements filter on node degree > maxDegree */
class MaxDegreeFilter(maxDegree: Long) extends NodeFilter[ExtendedVD[String, Int]] {
  override def isMatch(targetNode: ExtendedVD[String, Int]): Boolean = {
    targetNode.extension match {
      case Some(degree) => (degree <= maxDegree)
      case None   => true
    }
  }
}

class MinDegreeFilter(minDegree: Long) extends NodeFilter[ExtendedVD[String, Int]] {
  override def isMatch(targetNode: ExtendedVD[String, Int]): Boolean = {
    targetNode.extension match {
      case Some(degree) => (degree >= minDegree)
      case None   => true
    }
  }
}

class CitationFilter(minDegree: Int, maxDegree: Int, wordDistTarget: Set[String], 
    simCoefficient: Double = 0.1) extends NodeFilter[ExtendedVD[ String, (Int, Set[String])]] {
  override def isMatch(targetNode : ExtendedVD[ String, (Int, Set[String])]) : Boolean = {
    targetNode.extension match {
      case Some(extension) => {
        val degree = extension._1
        val wordDist = extension._2
        ((degree >= minDegree) && (degree <= maxDegree) && (MathUtils.jaccardCoeff(wordDist, wordDistTarget) > simCoefficient))
      }
      case None => true
    }
  }
}


/** Basic trait To define rank of an edge  and rank of a path
 *  Rank of a path = SomeFunction(of all edge in path)
 *  
 */
trait PathRank {
  def edgeRank(edge: PathEdge): Double
  def pathRank(path: List[PathEdge]): Double
}
 /* 
  // Adds topic distribution Array(topic strength) to each node ( topic id start from 1)
  def addAll[VD, ED](graph: Graph[VD, ED], topicsFileAll: String, sc: SparkContext) : Graph[ ExtendedVD[VD, Array[Double]], ED] = {
    val topicsRDD : RDD[(Long, Array[Double])]= TopicLearner.getYagoTopicsRDDAll(topicsFileAll, sc)
    graph.outerJoinVertices(topicsRDD)((vertexid, vertexData, topic) => new ExtendedVD(vertexData, topic))
  }
}
*/
/*
class PrepareFilter[VD, ED](filterType: String , filterArgs: Array[String], sc: SparkContext){
  val filterTypeLower = filterType.toLowerCase()
  
  def getExtendedGraph[VD, ED](g: Graph[VD, ED]): Graph[ExtendedVD[VD, Any], ED] = {
   
    filterTypeLower match {
      case "degree" => {
        val vertexDegreeRDD: VertexRDD[Int] = g.degree
        g.outerJoinVertices(vertexDegreeRDD)((id, value, degree) => new ExtendedVD(value, degree))
      }
      case "topic" => {
        if(filterArgs.length != 1) {
          println(" Need topic distribution file path")
          exit
        } else {
          val topicsFile = filterArgs(0)
          TopicLearner.addTopicsToGraphAll(g, topicsFile, sc).cache
        }
      }
      case "None" => g.mapVertices[ExtendedVD[VD, Int]]((id, value) => new ExtendedVD(value, None))
    }
  }
  
  def getFilterFunc[VD](destTopic: Array[Double]= Array.empty): NodeFilter[VD] = {
    val argsLen = args.length
    if(argsLen == 0) {
      new NodeFilter[ExtendedVD[String, Int]]
    } else { 
      val filterType = args(0).toLowerCase
      
      val filterObj = 
      filterType match {
        case "degree" => new DegreeFilter(maxDegree)(args(1).toInt)
        case "topic" => new TopicFilter(destTopic, args(1).toDouble)
        val filterObj = 
        
      }
    
      return filterObj
    }
  }
}
*/
/*
class IntentType(answerTypeTemp:String="", constraintTypeTemp:String ="", constraintValueTemp :String="") extends Serializable{
  
  val answerType: String = answerTypeTemp
  val constraintType : String = constraintTypeTemp
  val constraintValue: String = constraintValueTemp
  
  def modelIntent(): Unit = {
     
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
  }
}
*/

