/**
 *
 * @author puro755
 * @dJul 6, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.v3

import java.io.Serializable
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2Flat
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import gov.pnnl.aristotle.algorithms.mining.GraphPatternProfiler
import java.io.PrintWriter
import java.io.File
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2FlatInt

/**
 * @author puro755
 *
 */
class BatchMetrics(val id:Int) extends Serializable {

  //key:pattern value: support
  var pattern_in_this_batch : RDD[(List[Int], Long)] = null
  
  //key:pattern value: Set((node_degree,node_label))
  var pattern__node_association : RDD[(List[Int], Set[(Int,Int)])] = null
  var node_pattern_association : RDD[(Int, Set[List[Int]])] = null
    
  def updateBatchMetrics(batch_graph : Graph[KGNodeV2FlatInt, KGEdgeInt], writerSG:PrintWriter, args:Array[String]) 
  {
    this.pattern_in_this_batch = GraphPatternProfiler.get_sorted_patternV2Flat(batch_graph,
      writerSG, 2, args(1).toInt)
    this.pattern__node_association = GraphPatternProfiler.get_pattern_node_association_V2Flat(batch_graph,
      writerSG, 2, args(1).toInt)
    this.node_pattern_association = GraphPatternProfiler.get_node_pattern_association_V2Flat(batch_graph,
      writerSG, id, args(1).toInt)
  }
  
  def saveBatchMetrics()
  {
    pattern_in_this_batch.saveAsTextFile("PatternSummary" + System.nanoTime())
    pattern__node_association.saveAsTextFile("PatternNodeAssociation" + System.nanoTime())
    node_pattern_association.saveAsTextFile("NodePatternAssociation" + System.nanoTime())
  }
  
}