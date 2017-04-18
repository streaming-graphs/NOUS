package gov.pnnl.nous.pathSearch.Int

import org.apache.spark._
import gov.pnnl.nous.utils.MathUtils

object PathSearchIntDataTypes{
  type VertexEmb = Array[Double]
  type VertexId = Long
  type IsOutgoing = Boolean
  type IntEdge = (VertexId, Int, IsOutgoing)
  type Path = List[IntEdge]
  val defaultEdgeLabel = -1
}

import PathSearchIntDataTypes._
  
  abstract class PathFilter[VD]() extends Serializable {
    def isValidPair(prevNode: VD, currNode : VD) : Boolean //implements if pairs of nodes form a valid path 
    def isValidNode(currNode : VD) : Boolean // implements if a given node passes as valid path node
  }
  
  class DummyFilter() extends PathFilter[Int] {
    override def isValidPair(prevNode: Int, currNode: Int) = true
    override def isValidNode(currNode : Int): Boolean = true
  } 
  
  class DegreeFilter(maxDegree: Int) extends PathFilter[Int] {
    override def isValidPair(prevNode: Int, currNode: Int) = true
    override def isValidNode(currNode : Int): Boolean = {
      currNode <= maxDegree
    }
  } 
  
 
  class TopicFilter(maxDiv: Double) 
  extends PathFilter[VertexEmb] {
     override def isValidPair(prevNodeEmb:  VertexEmb, currNodeEmb : VertexEmb)  = {
      MathUtils.jensenShannonDiv(prevNodeEmb, currNodeEmb) < maxDiv
    }
    override def isValidNode(dummy :VertexEmb): Boolean = {
       true
    }
  }
  
  class TopicAndDegreeFilter(val maxDegree: Int, val maxPairDiv: Double) 
  extends PathFilter[(Int, VertexEmb)] {
    override def isValidPair(prevNodeEmb: (Int, VertexEmb), currNodeEmb : (Int, VertexEmb))  = {
      MathUtils.jensenShannonDiv(prevNodeEmb._2, currNodeEmb._2) < maxPairDiv
    }
    override def isValidNode(deg : (Int, VertexEmb)): Boolean = {
       deg._1 <= maxDegree
    }
  }
  /*
  class DestTopicAndDegreeFilter(val maxDegree: Int, val maxDstDiv: Double, val dstTopic: VertexEmb) 
  extends PathFilter[(Int, VertexEmb)] {
    override def isValidPair(currNodeEmb : (Int, VertexEmb), dummy: (Int, VertexEmb))  = {
      MathUtils.jensenShannonDiv(dstTopic, currNodeEmb._2) < maxDstDiv
    }
    override def isValidNode(degAndTopic : (Int, VertexEmb)): Boolean = {
       degAndTopic._1 <= maxDegree
    }
  }
  
  class DestPairTopicAndDegreeFilter(val maxDegree: Int, val maxPairDiv: Double, 
      val dstTopic: VertexEmb, val maxDstDiv: Double) 
  extends PathFilter[(Int, VertexEmb)] {
    override def isValidPair(prevNodeEmb: (Int, VertexEmb), currNodeEmb : (Int, VertexEmb))  = {
      (MathUtils.jensenShannonDiv(dstTopic, currNodeEmb._2) < maxDstDiv)  &&
      (MathUtils.jensenShannonDiv(prevNodeEmb._2, currNodeEmb._2) < maxPairDiv)
    }
    override def isValidNode(deg : Int): Boolean = {
       deg <= maxDegree
    }
  }
  * 
  */

