package gov.pnnl.nous.pathSearch.Int

import org.apache.spark._
import gov.pnnl.nous.utils.MathUtils

object PathSearchIntDataTypes{
  type VertexEmb = Array[Double]
  type VertexId = Long
  type IntEdge = (VertexId, Int)
  type Path = List[IntEdge]
  
  abstract class PathFilter[VD] extends Serializable() {
    def isValidPair : (VD, VD) => Boolean //implements if pairs of nodes form a valid path 
    def isValidNode : VD => Boolean // implements if a given node passes as valid path node
  }
  
  class DummyFilter() extends PathFilter[Int] {
    override def isValidPair(x: Int, y: Int) = true
    override def isValidNode(deg : Int): Boolean = true
  } 
  
  class DegreeFilter(maxDegree: Int) extends PathFilter[Int] {
    override def isValidPair(x: Int, y: Int) = true
    override def isValidNode(deg : Int): Boolean = {
      deg <= maxDegree
    }
  } 
  
 
  class TopicFilter(maxDiv: Double) extends PathFilter[VertexEmb] {
    override def isValidPair(x: VertexEmb, y: VertexEmb): Boolean =  (MathUtils.jensenShannonDiv(x,y) < maxDiv)
    override def isValidNode(x : VertexEmb): Boolean = true
  }
  
  class PairTopicAndDegreeFilter(val maxDegree: Int, val maxPairDiv: Double) 
  extends PathFilter[(Int, VertexEmb)] {
    override def isValidPair(prevNodeEmb: (Int, VertexEmb), currNodeEmb : (Int, VertexEmb))  = {
      MathUtils.jensenShannonDiv(prevNodeEmb._2, currNodeEmb._2) < maxPairDiv
    }
    override def isValidNode(deg : Int): Boolean = {
       deg <= maxDegree
    }
  }
  
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
}
