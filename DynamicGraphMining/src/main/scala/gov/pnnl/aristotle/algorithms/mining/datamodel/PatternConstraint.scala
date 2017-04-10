/**
 *
 * @author puro755
 * @dJun 29, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.datamodel

import java.io.Serializable

/**
 * @author puro755
 *
 */
class PatternConstraint extends Serializable {

  val base_type : Set[String] = Set("type:person","type:company","type:product","type:sup_mt")
}