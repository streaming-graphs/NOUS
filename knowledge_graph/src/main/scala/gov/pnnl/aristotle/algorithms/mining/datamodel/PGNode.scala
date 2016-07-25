package gov.pnnl.aristotle.algorithms.mining.datamodel

class PGNode(node_label :Int, support :Long, ptype : Int) extends Serializable {
  var pgprop : List[VertexProperty] = List.empty
  def getnode_label : Int = return node_label 
  def getsupport :  Long = return support
  def getptype : Int = return ptype
}