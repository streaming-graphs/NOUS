package gov.pnnl.aristotle.algorithms.mining.datamodel


case class KGEdgeInt(val label: Int, val datetime: Long) extends Serializable {
      
	def getlabel : Int = return label
	def getdatetime : Long = return datetime
	
}