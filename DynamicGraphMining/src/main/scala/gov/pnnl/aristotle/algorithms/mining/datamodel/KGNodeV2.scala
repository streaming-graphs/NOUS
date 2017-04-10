package gov.pnnl.aristotle.algorithms.mining.datamodel



/**
 * @author puro755
 *
 */
case class KGNodeV2(label: String, 
    pattern_map: Map[String, Long]) extends KGNode[String,Long] {

  def getInstanceCount = {
    getpattern_map.values.map(f => f)
  }
  
  def this(label: String, 
    pattern_map: Map[String, Long],
    annotation:List[VertexProperty]) = 
      {
    		this(label,pattern_map)
    		this.properties = annotation
      }
  
//  override def toString()
//  {
//    println(this.getlabel)
//    println(this.getpattern_map.toString)
//    println("Now prop")
//    this.getAnnotations.foreach(f=> println(f.id + "and" +f.property))
//    println("Now prop DONE")
//  }
}
