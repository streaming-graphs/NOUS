/**
 *
 */
package gov.pnnl.aristotle.algorithms.mining

import org.apache.spark.graphx.Graph
import java.io.PrintWriter
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge

/**
 * @author puro755
 *
 */
trait DynamicPatternGraph[A,B] extends Serializable {

  
    var TYPE: String 
  var SUPPORT: Int 
  var type_support :Int
  var input_graph: Graph[A, KGEdge]
  
  def init(graph: Graph[String, KGEdge], writerSG: PrintWriter,basetype:String,
      type_support : Int): DynamicPatternGraph[A,B] = {
    
    return this
  }

    /*
  def filterNonPatternSubGraphV2(updatedGraph: 
      Graph[gov.pnnl.aristotle.algorithms.KGNodeV2, KGEdge]): 
        Graph[gov.pnnl.aristotle.algorithms.KGNodeV2, KGEdge] =
    {
      return updatedGraph.subgraph(epred =>
        ((epred.attr.getlabel.equalsIgnoreCase(TYPE) != true) &&
          ((epred.srcAttr.asInstanceOf[gov.pnnl.aristotle.algorithms.KGNode[String, A]].getpattern_map.size != 0) ||
            (epred.dstAttr.asInstanceOf[gov.pnnl.aristotle.algorithms.KGNode[String, A]].getpattern_map.size != 0))))
    }
  */
    

    
}