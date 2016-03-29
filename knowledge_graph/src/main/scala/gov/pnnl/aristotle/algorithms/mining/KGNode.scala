package gov.pnnl.aristotle.algorithms

case class KGNode(label: String, pattern_map: Map[String, Set[PatternInstance]]) extends Serializable {

  //TODO: 
  ////List[Int] is used instead of Set[Int] to keep track of multiple edges with same 
  // pattern occurring on different times.

  def getlabel: String = return label
  def getpattern_map: Map[String, Set[PatternInstance]] = return pattern_map

  def getInstanceCount: Int = {
    getpattern_map.values.map(f => f.size).sum
  }
}