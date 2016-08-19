/**
 *
 * @author puro755
 * @dJun 17, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.datamodel

import java.io.Serializable


/**
 * @author puro755
 *
 */
class PatternTriple(subject: PatternNode, predicate:String,obj:PatternNode ) extends Serializable {

  def getsubject : PatternNode = return subject
  def getpredicate : String = return predicate
  def getobject : PatternNode = return obj
}