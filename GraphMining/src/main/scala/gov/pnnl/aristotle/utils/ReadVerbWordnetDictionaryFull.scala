package gov.pnnl.aristotle.utils

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.collection.SortedMap

object ReadVerbWordnetDictionaryFull {

  def main(args: Array[String]): Unit = {
    
    val wordnet_map = new HashMap[String, SortedMap[Double,String]]();
    for (line <- Source.fromFile("wsj_verb_yago_wordnetid_full.txt").getLines()) {
      if (line.startsWith("@") || line.startsWith("#") || line.isEmpty()) { ; }
      else {
        val linearray = line.toLowerCase().split("=>");
        var sorted_wordnet_id_map = SortedMap[Double,String]()
        val wordnet_id_list = linearray(1).split("\\t")
        wordnet_id_list.foreach(wordnet_id_entry => wordnet_map.put(linearray(0), 
            wordnet_map.getOrElse(linearray(0), SortedMap[Double,String]()) + 
            (wordnet_id_entry.split(":")(0).toDouble -> wordnet_id_entry.split(":")(1))))
        
      }
    }
  }
  
 
  def get_verv_wrodnet_dictionary_full(filename:String) : HashMap[String, SortedMap[Double,String]] =
    {
      val wordnet_map = new HashMap[String, SortedMap[Double, String]]();
      for (line <- Source.fromFile(filename).getLines()) {
        if (line.startsWith("@") || line.startsWith("#") || line.isEmpty()) { ; }
        else {
          val linearray = line.toLowerCase().split("=>");
          var sorted_wordnet_id_map = SortedMap[Double, String]()
          val wordnet_id_list = linearray(1).split("\\t")
          wordnet_id_list.foreach(wordnet_id_entry => wordnet_map.put(linearray(0),
            wordnet_map.getOrElse(linearray(0), SortedMap[Double, String]()) +
              (wordnet_id_entry.split(":")(0).toDouble -> wordnet_id_entry.split(":")(1))))
        }
      }
      return wordnet_map

    }
  
}