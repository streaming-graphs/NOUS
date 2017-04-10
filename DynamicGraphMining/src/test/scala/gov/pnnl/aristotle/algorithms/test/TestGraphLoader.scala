package gov.pnnl.aristotle.algorithms.test
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.{VertexRDD,VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import util.control.Breaks._
import scala.Array.canBuildFrom

object TestGraphLoader {

  
	 val sparkConf = new SparkConf().setAppName("Test GraphX").setMaster("local") 
	 val sc = new SparkContext(sparkConf)
	 
  
  def main(args: Array[String]): Unit = {

	   
/*
 *    Read yago facts files
 */	   
	   
    //val filename = "/sumitData/myprojects/AIM/yagoWikipediaInfo.ttl"
      val filename = "/sumitData/myprojects/AIM/aristotle-dev/knowledge_graph/yagowikiinfo.ttl"
    var subjectArray = new Array[(Long,String)](0);
    var predicateArray = new Array[Edge[String]](0);
    var objectArray = new Array[(Long,String)](0);
    
    
    for (line <- Source.fromFile(filename).getLines()) 
    {
      breakable
      {
      if(line.startsWith("@prefix") || line.startsWith("#") || line.isEmpty()) break
      val lineArray = line.split("\\s+")
      if (lineArray.length < 2) {
        print("Invalid line: " + line)
      }
      val Subject = lineArray(0)
      val Predicate = lineArray(1)
      val Object = lineArray(2)

      subjectArray = subjectArray :+ (Subject.hashCode().toLong,Subject);
      predicateArray = predicateArray :+ Edge(Subject.hashCode().toLong,Object.hashCode().toLong,Predicate);
      objectArray = objectArray :+ (Object.hashCode().toLong,Object);

  
        
      }
          }
    print(subjectArray.length);
    subjectArray = subjectArray.toList.distinct.toArray;
    predicateArray = predicateArray.toList.distinct.toArray;
    objectArray = objectArray.toList.distinct.toArray;

    val allVertexArray = subjectArray ++ objectArray;

    
    val vertextRDD: RDD[(VertexId, String)] = sc.parallelize(allVertexArray)
    val edgeRDD: RDD[Edge[String]] = sc.parallelize(predicateArray)
    var graph = Graph(vertextRDD, edgeRDD)
    
    
    val cnt = graph.edges.filter { e => e.attr.toString() == "gn:locatedIn" }.count
    val av: VertexRDD[String] = graph.vertices.filter { case (id, (name)) => name == "<gcmd/tag:pnnl.gov,2013:rdesc/location/CONTINENT/ASIA/EASTERN%20ASIA/CHINA>" }
    
    val av2: VertexRDD[String] = graph.vertices.filter { case (id, (name)) => name.toLowerCase().contains("apple") }
    
    print("\n locationIN " + av.first);
  }

}