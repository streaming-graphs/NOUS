package gov.pnnl.aristotle.utils

import scala.io.Source
import scala.collection.mutable.HashMap

object ReadVerbWordnetDictionaryTop1 {

  def main(args: Array[String]): Unit = {

    val wordnet_map = new HashMap[String, String]();
    for (line <- Source.fromFile("wsj_verb_yago_wordnetid_top1.txt").getLines()) {
      if (line.startsWith("@") || line.startsWith("#") || line.isEmpty()) { ; }
      else {
        val linearray = line.toLowerCase().split("=>");
        wordnet_map.put(linearray(0), linearray(1).trim());
      }
    }
  }
  
  def get_verv_wrodnet_dictionary_top1(filename:String) : HashMap[String, String] =
  {
    val wordnet_map = new HashMap[String, String]();
    for (line <- Source.fromFile("wsj_verb_yago_wordnetid_top1.txt").getLines()) {
      if (line.startsWith("@") || line.startsWith("#") || line.isEmpty()) { ; }
      else {
        val linearray = line.toLowerCase().split("=>");
        wordnet_map.put(linearray(0), linearray(1).trim());
      }
    }
    
    return wordnet_map
  }

}