package gov.pnnl.aristotle.algorithms.entity

//import org.scalatest._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.Map
import breeze.linalg._
import breeze.numerics._

//import gov.pnnl.aristotle.algorithms.entity.ColEntityDisamb
import gov.pnnl.aristotle.algorithms.{ReadHugeGraph}
import gov.pnnl.aristotle.utils.{NLPTripleParser, NLPTriple, 
  NLPTripleWithTimeWithUrl, MentionData, MatchStringCandidates}

//import org.json4s._
//import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods._
//import org.json4s.native.JsonMethods._
//import org.json4s.native.Serialization._
//import org.json4s.native.Serialization

import collection.immutable.ListMap
import scala.collection.mutable.LinkedHashMap 
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json._
import java.io._

object RunColDisambRefer{
  
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("EntityDisamb").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("starting from main")
    
    if(args.length != 5){
        println("provide <path to graph> <path to new_triples file> <StringPhraseMatchThreshold> <MentionToEntityMatchThreshold> <BenchMark>")
      exit
    }
    
   
    val graphFile = args(0)
    var NLPTripleFile = args(1)
    val phraseMatchThreshold = args(2).toDouble
    val mentionToEntityMatchThreshold = args(3).toDouble
    val benchMark = scala.io.Source.fromFile(args(4)).getLines.toList.map(s => Map(s.split(";")(1) -> s.split(";")(2))).reduce(_++_)
    //println("Reading triple file")
    //val allTriples: List[List[NLPTriple]] = NLPTripleParser.readTriples(NLPTripleFile, sc)
    // println("No of blocks=" + allTriples.size + "\n")
    //var allTriplesByUrl: List[List[NLPTripleWithTimeWithUrl]] = 
    //  NLPTripleParser.readTriplesWithTimestampWithUrl(NLPTripleFile, sc).values.toList
    //println("No of unique triple blocks =" + allTriplesByUrl.size + "\n")
    
    println("Reading graph")
    val g: Graph[String, String] = ReadHugeGraph.getGraph(args(0), sc)
    
    println("geting aliases")
    val verticesWithAlias = MatchStringCandidates.constructVertexRDDWithAlias(g)
    println("Done getting aliases" + verticesWithAlias.count + ", starting disambiguation\n")
   
    
    val colEntityDisObj = new ColEntityDisamb[String, String]
    var userInput: String = NLPTripleFile

    /**case class Contents(sentence_id:Int, entities: List[String], triples:List[String])
    case class NLPoutput(doc_name:String, nlp_output:List[Contents])
    def jsonStrToMap(jsonStr: String): NLPoutput = {
      implicit val formats = DefaultFormats
      parse(jsonStr).extract[NLPoutput]
    
    }
    */

    val lines = scala.io.Source.fromFile(userInput).getLines.mkString 
    val nlp_output = JSON.parseFull(lines).get.asInstanceOf[Map[String, Any]]("nlp_output").asInstanceOf[List[Map[String, Any]]]
    //val originalOutput = jsonStrToMap(lines)

      
    def reverse(a: String) : Map[String, MentionData] = Map(a.split(":")(1) -> new MentionData(a.split(":")(0), 0.6))

    //val preThree = nlp_output.take(3).drop(2)
    val preThree = nlp_output.take(20)   // at most 20 abstracts
    var total = 0
    var correct = 0
    var wrong = 0
    var docid = 0
    //var result = LinkedHashMap("mention : ActualEntity" -> "WronglyMappedEntity : Score")    
    var wrongresult = ListBuffer("mention : ActualEntity : WronglyMappedEntity : Score")
    
    val writer = new PrintWriter(new File("paperTriples.txt"))
    for(entry <- preThree){
      println(docid)
      
      val allMentionsWithData: Map[String, MentionData] = entry("entities").asInstanceOf[List[String]].map(reverse).reduce(_++_) 
      allMentionsWithData.foreach(v => println(v._1 + "--(type, evidence weight)-->"+ v._2.toString))  
      
      val mentionMatches: Map[String, ((VertexId, String), Double)] = colEntityDisObj.disambiguate(
          allMentionsWithData, g, verticesWithAlias, phraseMatchThreshold, mentionToEntityMatchThreshold, 0.00001)
      
      
          
      //println
      //println("Disambiguation completed:")
      //mentionMatches.foreach((MentionWithMatch) => println(
      //  MentionWithMatch._1 , "=>" , MentionWithMatch._2._1._1,  MentionWithMatch._2._1._2, 
      //  MentionWithMatch._2._2)) 
      ///** the following is used for the accuracy statistics
      for (item <- mentionMatches) {
        total += 1
        if (item._2._2 == 0) {
            var word = ""
            if (item._1.contains(":")) {
              word = item._1.split(":")(1)
            } else {
              word = item._1
            }
            if (benchMark(word) == "nous") {
              correct += 1
            } else {
              wrong += 1
              //result += ((word + " : " + benchMark(word)) -> ("nous" + " : " + item._2._2.toString))
              wrongresult.append(List(word, benchMark(word), "nous", item._2._2.toString).mkString(" : "))
            }
            writer.write("<paperID"+docid + ">\t" + "<hasMention>\t" + "<nous:" + word + ">\n")
        } else {
           val mentionWord = item._1.split(":")(1)
           if (benchMark(mentionWord) == item._2._1._2) {
             correct += 1
           } else {
             wrong += 1
             //result += ((mentionWord + " : " + benchMark(mentionWord)) -> (item._2._1._2 + " : " + item._2._2.toString))
             wrongresult.append(List(mentionWord, benchMark(mentionWord), item._2._1._2, item._2._2.toString).mkString(" : "))
           }
           writer.write("<paperID"+docid + ">\t" + "<hasMention>\t" +"<"+item._2._1._2 + ">\n")
        }
      }
      docid += 1
      //*/
    }
    writer.close()
 
    //println
    //println("total mentions: " + total)
    //println("correct mapped: " + correct)
    //println("wrong mapped: " + wrong)
    //println("the accuracy is: " + correct.toDouble / total) 
    //println(wrongresult.size)
    //wrongresult.foreach(item => println(item)) 

  }
}
