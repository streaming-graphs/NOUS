/**
 *
 */
package gov.pnnl.aristotle.algorithms

import gov.pnnl.aristotle.algorithms.WordNet_Mapper_Util
import scala.io.Source
/**
 * @author puro755
 *
 */
object testWordNetMapper1 {

  def main(args: Array[String]): Unit = {

    val path: String = "predicate_sutanay"
    var predicate_list: List[String] = List.empty
    for (line <- Source.fromFile(path).getLines()) {
      if (line.startsWith("@") || line.startsWith("#") || line.isEmpty()) { ; }
      else {
      	predicate_list = predicate_list :+ line
      }

    }
    
    predicate_list.foreach(predicate =>{
      val canonical_predicate = WordNet_Mapper_Util.getCanonicalVerb(predicate)
      val wordnet_id = WordNet_Mapper_Util.getCorrespondingWordNetIDs(Map(canonical_predicate-> ""))
      print(predicate+":"+canonical_predicate)
      wordnet_id.foreach(f=>f._2.foreach(entry => print(entry._1+":"+entry._2)))
      println()
    })
    
  }

}  