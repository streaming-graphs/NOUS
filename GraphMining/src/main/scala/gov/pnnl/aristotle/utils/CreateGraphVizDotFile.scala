package gov.pnnl.aristotle.utils

import scala.io.Source
import java.io.PrintWriter
import java.io.File


object CreateGraphVizDotFile {
  
  
  def drawTTLFile(inputPath:String,outputPath:String)
  {
    var output_dot = new PrintWriter(new File(outputPath))
    output_dot.println("digraph data {")
    output_dot.println("rankdir=LR;")
    for (line <- Source.fromFile(inputPath).getLines()) {
      if (line.startsWith("@") || line.startsWith("#") || line.isEmpty()) { ; }
      else {
        appendQuadrupleToDotFile(line,output_dot)
      }
    }
    output_dot.println("}")
    output_dot.flush()
  }
  

  def main(args: Array[String]): Unit = {

    //drawTTLFile("GraphMineInputTime5.txt", "GraphMineInputTime5.dot")
    //drawDepGraph("DependencyGraphEdges396174451444519/part-00000")
    drawDepGraph(args(0))
  }

  def drawDepGraph(filepath : String)
  {

    var entity_Map: Map[String, Long] = Map.empty
    var all_patterns_dot = new PrintWriter(new File("all_patternint.dot"))
    all_patterns_dot.println("digraph data {")
    all_patterns_dot.println("rankdir=LR;")
    var all_pattern_counter = 0
    var frequent_pattern_counter = 0
    val fillcolor_map = Map((-1 -> "red"), (0 -> "forestgreen"), (1 -> "gold3"), (2 -> "cyan3"))
    //-1 infrequent, 0 promixing, 1 : closed, 2, redundant
    var allnodes: Map[String, Int] = Map.empty

    for (line <- Source.fromFile(filepath).getLines()) {
      if (line.startsWith("@") || line.startsWith("#") || line.isEmpty()) { ; }
      else {
        val clean_line_array = line.replaceAll("\\(", "").replaceAll("\\)", "").split("List")
        val src = clean_line_array(1)
        val dst = clean_line_array(2)
        allnodes = allnodes + (dst.split("#")(0) -> dst.split("#")(1).replaceAll(",", "").toInt)

        /*
         * Avoid making self loop
         */
        if (!src.split("#")(0).equalsIgnoreCase(dst.split("#")(0))) {
          all_patterns_dot.println('"' + src.split("#")(0) + '"' + " -> " + '"' + dst.split("#")(0) +
            '"' + " [label=" + '"' + "part_of" + '"' + "]")
          all_pattern_counter += 1

        }
      }
    }
    allnodes.foreach(f => println(f._1 + " " + f._2.toInt))
    println(allnodes.size)
    allnodes.foreach(n => all_patterns_dot.println('"' + n._1 + '"' + " [style=filled, fillcolor=" + fillcolor_map.getOrElse(n._2, "red") + "]"))
    all_patterns_dot.println("}")
    all_patterns_dot.flush()

    all_patterns_dot.flush()

  }
  
  
  def drawGraphMiningOutputFile(filepath : String)
  {
    val path : String = filepath // "/sumitData/work/myprojects/AIM/aristotle-dev/knowledge_graph/traphMiningOutput.txt"
    var entity_Map :Map[String,Long] = Map.empty
    var all_patterns_dot = new PrintWriter(new File("all_pattern.dot"))
    var frequent_patterns = new PrintWriter(new File("frequent_pattern.dot"))
    //label="Graph";
    //labelloc=top;
    //labeljust=left;
    all_patterns_dot.println("digraph data {")
    all_patterns_dot.println("rankdir=LR;")
    frequent_patterns.println("digraph data {")
    frequent_patterns.println("rankdir=LR;")
    var all_pattern_counter = 0
    var frequent_pattern_counter = 0
    
    
  for(line <- Source.fromFile(path).getLines()) {
  	if (line.startsWith("@") || line.startsWith("#") || line.isEmpty() ) { ; }
  	else
  	{
  	  if(line.startsWith("All:"))
  	  {
      	appendQuadrupleToDotFile(line.replaceAll("All:", "").
  	  	    replaceAll("\t+", "\t"),all_patterns_dot)
  	    all_patterns_dot.println("}")
  	    all_patterns_dot.flush()
  	    all_pattern_counter += 1
  	  }
  	  if(line.startsWith("Frq:"))
  	  {
  	    frequent_patterns.println(s"subgraph clusterstep$frequent_pattern_counter {")
  	    appendQuadrupleToDotFile(line.replaceAll("Frq:", "").
  	  	    replaceAll("\t+", "\t"),frequent_patterns)
  	  	frequent_patterns.println("}")  
  	  	frequent_pattern_counter += 1

  	  }
  	}
  }
    
    all_patterns_dot.println("}")
    frequent_patterns.println("}")
    
    all_patterns_dot.flush()
    frequent_patterns.flush()

  }
  
  def appendQuadrupleToDotFile(quadruple : String, file : PrintWriter) =
  {
    val quadruple_array = quadruple.trim().replaceAll("\"", "").split("\\t")
    
    val number_of_edge = quadruple_array.length/3
    var i = 0
    // A Pattern can be of any length so take set of 3 and draw as one line
    for(i <- 0 to  number_of_edge-1)
    {
        var base_index = i * 3
        file.println('"' + quadruple_array(base_index) + '"' + " -> " + '"' + quadruple_array(base_index + 2) +
          '"' + " [label=" + '"' + quadruple_array(base_index +1) + '"' + "]")
      }
    
  }
}