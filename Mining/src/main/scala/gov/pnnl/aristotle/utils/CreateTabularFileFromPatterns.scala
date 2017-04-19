package gov.pnnl.aristotle.utils

import scala.io.Source
import java.io.PrintWriter
import java.io.File

object CreateTabularFileFromPatterns {

  def main(args: Array[String]): Unit = {
    
    
    val path : String = "/sumitData/work/myprojects/AIM/aristotle-dev/knowledge_graph/GraphMiningOutput.txt"
    var entity_Map :Map[String,Long] = Map.empty
    val all_patterns_dot = new PrintWriter(new File("all_pattern_2k13.tsv"))
    val frequent_patterns = new PrintWriter(new File("frequent_pattern_2k13.tsv"))
    
    var all_pattern_counter = 0
    var frequent_pattern_counter = 0
  for(line <- Source.fromFile(path).getLines()) {
  	if (line.startsWith("@") || line.startsWith("#") || line.isEmpty() ) { ; }
  	else
  	{
  	  if(line.startsWith("All:"))
   	  	appendQuadrupleToTableFile(line.replaceAll("All:", "").
  	  	    replaceAll("\t+", "\t"),all_patterns_dot)
  	  if(line.startsWith("Frq:"))
  	    appendQuadrupleToTableFile(line.replaceAll("Frq:", "").
  	  	    replaceAll("\t+", "\t"),frequent_patterns)
  	}
  }
    
    all_patterns_dot.flush()
    frequent_patterns.flush()
}

  def appendQuadrupleToTableFile(quadruple : String, file : PrintWriter) =
  {
    val separtor = quadruple.trim().lastIndexOf("\t")
    file.println(quadruple.trim().substring(0,separtor).replaceAll("\t", "_")+ "\t" + quadruple.trim().substring(separtor+1))
  }
  
}