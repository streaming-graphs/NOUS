package gov.pnnl.aristotle.algorithms.mining.datamodel



/**
 * @author puro755
 *
 */
case class KGNodeV2FlatInt(label: Int, 
    pattern_map: Array[(List[Int], Long)],properties:List[VertexProperty]) {

  def getInstanceCount = {
    getpattern_map.map(f => f._2)
  }

  def getVertextPropLableArray : List[Int] =
    {
      this.properties.map( vprop => vprop.property_label )
    }
  def getlabel : Int = return label
  def getProperties: List[VertexProperty] = return properties
  def getpattern_map: Array[(List[Int], Long)] = return pattern_map
  def getpattern_map_keys: Array[List[Int]] = return pattern_map.map(pattern=>pattern._1)
  def getpattern_map_filter(valid_patterns : Set[List[Int]]): Array[(List[Int], Long)] = return pattern_map.filter(entry
      =>valid_patterns.contains(entry._1))

}
