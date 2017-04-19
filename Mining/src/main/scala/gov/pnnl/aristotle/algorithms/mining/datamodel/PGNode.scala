package gov.pnnl.aristotle.algorithms.mining.datamodel

class PGNode(node_label :List[Int], support :Long, ptype : Int) extends Serializable {
  var pgprop : List[List[Int]] = List.empty
  def getnode_label : List[Int] = return node_label 
  def getsupport :  Long = return support
  def getptype : Int = return ptype
}