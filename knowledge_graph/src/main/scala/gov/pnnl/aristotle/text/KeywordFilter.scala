package gov.pnnl.aristotle.text

object KeywordFilter {
  val keywords = Set[String]()
  def filter(s:String): Boolean = keywords.contains(s)
}
