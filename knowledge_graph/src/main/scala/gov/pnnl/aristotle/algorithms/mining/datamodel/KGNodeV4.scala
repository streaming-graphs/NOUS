/**
 *
 * @author puro755
 * @dAug 22, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.datamodel

import java.io.Serializable

/**
 * @author puro755
 *
 */
class KGNodeV4(label: Int, 
    pattern_map: Array[(List[Int], Set[PatternInstance])],properties:List[VertexProperty]) extends Serializable {
  //TODO: Boolean should go to an Instance, not the node

  def getlabel: Int = return label
  def getProperties: List[VertexProperty] = return properties
  def getpattern_map: Array[(List[Int], Set[PatternInstance])] = return pattern_map

}