package gov.pnnl.aristotle.algorithms


case class KGEdge(label: String, datetime: Long) extends Serializable {
      
	def getlabel : String = return label
	def getdatetime : Long = return datetime
	
    }