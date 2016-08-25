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
class PatternInstanceNode(val pattern: List[Int],
    val instance: PatternInstance, val timestamp: Long) extends Serializable {

  /*
   * first Int is the pattern node key,
   * second Int is the instance node key
   */
  var pattern_instance_map : Map[Int,Int] = Map.empty
  
}