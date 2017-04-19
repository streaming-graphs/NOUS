package gov.pnnl.aristotle.algorithms.mining.datamodel

import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNode
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNode
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternInstance

/**
 * @author puro755
 *
 */
case class KGNodeV1(label: String, 
    pattern_map: Map[String, Set[PatternInstance]]) extends
    KGNode[String,Set[PatternInstance]] {

  def getInstanceCount: Long = {
    getpattern_map.values.map(f => f.size).sum
  }
}