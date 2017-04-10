package gov.pnnl.aristotle.algorithms.mining.datamodel


case class KGEdge(label: String, datetime: Long) extends Serializable {
      
	def getlabel : String = return label
	def getdatetime : Long = return datetime
	
}