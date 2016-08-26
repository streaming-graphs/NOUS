/**
 *
 * @author puro755
 * @dAug 24, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.datamodel

import java.io.Serializable

/**
 * @author puro755
 *
 */
class PatternInstanceNode(val pattern_id: List[Int],
    val pattern_edge: List[Int] ,val instance_edge: PatternInstance,
    val pattern_instance_map : Map[Int,Int],val timestamp: Long) extends Serializable {

  /*
   * first Int is the pattern node key,
   * second Int is the instance node key
   */
  
}