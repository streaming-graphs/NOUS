package gov.pnnl.aristotle.algorithms.mining.datamodel

class PGNode(node_label :String, support :Long) extends Serializable {
  def getnode_label : String = return node_label 
  def getsupport :  Long = return support
}