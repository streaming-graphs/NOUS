/**
 *
 * @author puro755
 * @dMay 5, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms

import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.{FieldSerializer, DefaultFormats}
import net.liftweb.json.Serialization.write



/**
 * @author puro755
 *
 */
object LASPatternJSONBuilder {


case class NOUSLASAnswer(`type`: String, pattern: Array[String], response: Response)  
case class Response(is_temporal: String, _comment: String, data: Array[Data])
case class Data(timestamp: String, text: String, graph: Array[ResponseGraph],frequency:String)
case class ResponseGraph(graphID: String, graph_item: Array[String])
  

  

  def main(args: Array[String]): Unit = {

    val graph = ResponseGraph("graph01", Array("Drone\ta near midair collision\tcommercial airliner",
      "commercial airliner\tfly over\tFlorida"))

    val data = Data("20151010", "test string in data", Array(graph),"-1")
    val res = Response("false", "test comment in response", Array(data))

    val ans = NOUSLASAnswer("4", Array("amazon"), res)

    // create a JSON string from the Person, then print it
    implicit val formats = net.liftweb.json.DefaultFormats
    val jsonString = write(ans)
    println(jsonString)

  }

  def getMakeJSONRDD(entitykey: String, 
      patternrdd: RDD[(String, List[(String, Long)])],qtype :Int) : String = {

    var alldata: Array[Data] = Array.empty

    patternrdd.collect.foreach(entry => {
      //each entry is like this :
      // (aerialtronics	manufacturer of	high performance multirotor aerial platforms		aerialtronics	announces	two new distribution partners	,List(20151205, 4, 20151213, 4))

      val pattern_num_tabs_array = entry._1.replaceAll("\t+", "\t").split("\t")
      var graphitem = ""
      if (pattern_num_tabs_array.length > 3) {
        for (i <- 0 to pattern_num_tabs_array.length - 1) {
          graphitem = graphitem + pattern_num_tabs_array(i) + "\t"
          if ((i % 3 == 2) && (i != pattern_num_tabs_array.length - 1))
            graphitem = graphitem + ","
        }
      }
      var id = 0
      entry._2.foreach(time_count_el => {
        var graph = ResponseGraph(s"graph$id", Array(graphitem))
        val data = Data(time_count_el._1, "test string in data", Array(graph), time_count_el._2.toString)
        alldata = alldata :+ data
      })
    })
    val res = Response("false", "test comment in response", alldata)
      val ans = NOUSLASAnswer(qtype.toString, entitykey.split("_"), res)
      implicit val formats = net.liftweb.json.DefaultFormats
      write(ans)
  }
 
    def getMakeJSONRDDDefault(entitykey: String, 
      patternrdd: RDD[(String, Long)],qtype :Int) : String = {

    var alldata: Array[Data] = Array.empty
var id = 0
    patternrdd.collect.foreach(entry => {
      //each entry is like this :
      // (aerialtronics	manufacturer of	high performance multirotor aerial platforms		aerialtronics	announces	two new distribution partners	,List(20151205, 4, 20151213, 4))

      val pattern_num_tabs_array = entry._1.replaceAll("\t+", "\t").split("\t")
      var graphitem = ""
      if (pattern_num_tabs_array.length > 3) {
        for (i <- 0 to pattern_num_tabs_array.length - 1) {
          graphitem = graphitem + pattern_num_tabs_array(i) + "\t"
          if ((i % 3 == 2) && (i != pattern_num_tabs_array.length - 1))
            graphitem = graphitem + ","
        }
      }
      
      var graph = ResponseGraph(s"graph$id", Array(graphitem))
      val data = Data("1L", "test string in data", Array(graph), entry._2.toString)
        alldata = alldata :+ data
        id = id +1	
    })
    val res = Response("false", "test comment in response", alldata)
      val ans = NOUSLASAnswer(qtype.toString, entitykey.split("_"), res)
      implicit val formats = net.liftweb.json.DefaultFormats
      write(ans)
  }
  
  
    def getMakeJSONSearchRDD(search_answer: (String, List[String])) : String = {

    var graphitem: String = ""
    val keyarray = search_answer._1.split("_")
    val question_type = keyarray(0)
    val entitykey = keyarray.slice(1,keyarray.length)
    val all_answers = search_answer._2
    for(i <- 0 to all_answers.length - 1)
    {
      if(i==0)
        graphitem = (all_answers(i)+i).replaceAll("\t", i+"\t").replaceAll(",", i+",")
      else 
        graphitem = graphitem + "\n" + (all_answers(i)+i).replaceAll("\t", i+"\t").replaceAll(",", i+",")
    }
    var graph = ResponseGraph(search_answer._1, Array(graphitem))
    val data = Data("", "test string in data", Array(graph), "") 

    val res = Response("false", "test comment in response", Array(data))
      val ans = NOUSLASAnswer(question_type.toString, entitykey, res)
      implicit val formats = net.liftweb.json.DefaultFormats
      val str= write(ans)
      return str
  }
    
}