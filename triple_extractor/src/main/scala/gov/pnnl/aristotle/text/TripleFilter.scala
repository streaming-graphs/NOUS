package gov.pnnl.aristotle.text

import gov.pnnl.aristotle.text.Triple

object TripleFilter {
  val commonVerbs = Set("is", "was")

  def filterRelation(relation: String): Boolean = { 
    // if (badVerbs.contains(relation) || relation.startsWith("'s")) 
    if (relation.startsWith("'s")) 
      false
    else
      true
  }   
  
  def filterEntities(
    namedPhrases: Set[String], 
          sub: String, 
          obj: String): Boolean = { 
    namedPhrases.exists(n => sub.contains(n)) 
  }   

  def filterCommonVerbs(triple: Triple, namedPhrases: Set[String]): Boolean = {
    if (commonVerbs.contains(triple.pred)) {
      if (namedPhrases.exists(n => triple.obj.contains(n))) 
        true
      else 
        false
    }
    else 
      true
  }

  def filter(triple: Triple, namedPhrases: Set[String]): Boolean = {
    if (filterRelation(triple.pred) == false)
      return false
    if (filterEntities(namedPhrases, triple.sub, triple.obj) == false) 
      return false
    if (filterCommonVerbs(triple, namedPhrases) == false) 
      return false
    true 
  }
}
