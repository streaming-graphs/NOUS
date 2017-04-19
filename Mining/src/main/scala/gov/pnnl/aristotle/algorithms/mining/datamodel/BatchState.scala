/**
 *
 */
package gov.pnnl.aristotle.algorithms.mining.datamodel

import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import org.apache.spark.graphx.Graph
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge

/**
 * @author puro755
 *
 */
class BatchState(inputgraph : Graph[String, KGEdge], start_time : Long, batch_size : Long, val id:Int)
	extends Serializable {
	//private var inputgraph  : Graph[KGNode, KGEdge] = null;
	
	def getinputgraph : Graph[String, KGEdge] = return inputgraph
	def getstart_time : Long = return start_time
	def getbatch_size : Long = return batch_size
	  
	//Setter
	//def setinputgraph_= (graph:Graph[KGNode, KGEdge]):Unit = inputgraph = graph 
}