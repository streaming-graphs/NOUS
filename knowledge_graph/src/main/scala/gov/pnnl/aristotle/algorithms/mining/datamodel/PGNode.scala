package gov.pnnl.aristotle.algorithms.mining.datamodel

class PGNode(node_label :String, support :Long, ptype : Int) extends Serializable {
  var pgprop : List[VertexProperty] = List.empty
  def getnode_label : String = return node_label 
  def getsupport :  Long = return support
  def getptype : Int = return ptype
}