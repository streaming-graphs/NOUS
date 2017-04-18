package gov.pnnl.nous

object GraphProp {

  val edgeLabelNeighbourEntity = "linksTo"
  val edgeLabelNodeType = "rdf:type"
  val edgeLabelNodeAlias : List[String]= List("rdfs:label", "skos:prefLabel", "isPreferredMeaningOf")
  val aliasSep: String = " <Alias> "
  val typeSep :String = " <Type> "

}