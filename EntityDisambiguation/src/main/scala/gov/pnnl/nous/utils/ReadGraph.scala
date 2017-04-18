package gov.pnnl.nous.utils


import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import scala.Array.canBuildFrom


object ReadGraph {

  def isValidLineFromGraphFile(ln : String) : Boolean ={
    ((ln.startsWith("3210#") ==false) &&  (ln.startsWith("@") ==false) && (ln.startsWith("#")==false) && (ln.isEmpty()==false))
  }
  
  def getFieldsFromLine(line :String, sep: String = "\t") : Array[String] = {
    return line.toLowerCase().replaceAllLiterally("<", "").replaceAllLiterally(">", "").replace(" .", "").split(sep).map(str => str.stripPrefix(" ").stripSuffix(" "));
  }
  
  def getGraph(filename : String, sc : SparkContext): Graph[String, String] = {
    println("starting graph read");
    val triples: RDD[(String, String, String)] =
      sc.textFile(filename).filter(ln => isValidLineFromGraphFile(ln)).map { line =>
        val fields = getFieldsFromLine(line);
        if (fields.length == 4)
          (fields(1), fields(2), fields(3))
        else if(fields.length == 3)
          (fields(0), fields(1), fields(2))
        else {
          println("Exception reading graph file line", line)
          ("None", "None", "None")
        }
      }.cache


    val edges = triples.map(triple => Edge(triple._1.hashCode().toLong, triple._3.hashCode().toLong, triple._2)).distinct
    val vertices = 
      triples.flatMap(triple => Array((triple._1.hashCode().toLong, triple._1), (triple._3.hashCode().toLong, triple._3)))

    println("Read complete, Building graph");
    val graph = Graph(vertices, edges);
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
 
    return graph
  }
  
 
  
}

