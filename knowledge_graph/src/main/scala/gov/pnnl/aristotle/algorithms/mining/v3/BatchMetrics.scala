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

/**
 * @author puro755
 *
 */
class BatchMetrics(val id:Int) extends Serializable {

  var pattern_in_this_batch : RDD[(String, Long)] = null
  var pattern__node_association : RDD[(String, Set[String])] = null
  var node_pattern_association : RDD[(String, Set[String])] = null
    
  def updateBatchMetrics(batch_graph : Graph[KGNodeV2Flat, KGEdge], writerSG:PrintWriter, args:Array[String]) 
  {
    this.pattern_in_this_batch = GraphPatternProfiler.get_sorted_patternV2Flat(batch_graph,
      writerSG, 2, args(1).toInt)
    this.pattern__node_association = GraphPatternProfiler.get_pattern_node_association_V2Flat(batch_graph,
      writerSG, 2, args(1).toInt)
    this.node_pattern_association = GraphPatternProfiler.get_node_pattern_association_V2Flat(batch_graph,
      writerSG, 2, args(1).toInt)
  }
  
  def saveBatchMetrics()
  {
    pattern_in_this_batch.saveAsTextFile("PatternSummary" + System.nanoTime())
    val writer_pattern = new PrintWriter(new File("GraphMiningPattern.txt"))
    pattern_in_this_batch.collect.foreach(p => writer_pattern.println(p._1 + ":" + p._2))
    writer_pattern.flush()
    pattern__node_association.saveAsTextFile("PatternNodeAssociation" + System.nanoTime())
    node_pattern_association.saveAsTextFile("NodePatternAssociation" + System.nanoTime())
  }
  
}