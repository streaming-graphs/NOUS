/**
 *
 */
package gov.pnnl.aristotle.algorithms.mining.datamodel

import gov.pnnl.aristotle.algorithms.mining.datamodel.VertexProperty

/**
 * @author puro755
 *
 */
trait KGNode[T1,T2] extends Serializable {

  val label: String
  var properties: List[VertexProperty] = List.empty
  val pattern_map: Map[T1, T2]

  def getlabel: String = return label
  def getProperties : List[VertexProperty] = return properties
  def getpattern_map: Map[T1, T2] = return pattern_map
}