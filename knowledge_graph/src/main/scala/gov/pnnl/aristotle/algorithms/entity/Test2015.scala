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


import collection.immutable.ListMap
import scala.collection.mutable.LinkedHashMap 
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json._
import java.io._

object RunTest2015 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Test2015").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("startig from main")

    if (args.length != 4) {
      println("provide <path to graph> <path to new_triples file> <StringPhraseMatchThreshold> <MentionToEntityMatchThreshold>")
    }
    
    val graphFile = args(0)
    var NLPTripleFile = args(1)
    val phraseMatchThreshold = args(2).toDouble
    val mentionToEntityMatchThreshold = args(3).toDouble

    println("Reading graph")
    val g: Graph[String, String] = ReadHugeGraph.getGraph(args(0), sc)

    println("getting aliases")
    val verticesWithAlias = MatchStringCandidates.constructVertexRDDWithAlias(g)
    println("Done getting aliases" + verticesWithAlias.count + ", starting disambiguation\n")

    val colEntityDisObj = new ColEntityDisamb[String, String]
    var userInput: String = NLPTripleFile

    val lines = scala.io.Source.fromFile(userInput).getLines.mkString 
    val nlp_output = JSON.parseFull(lines).get.asInstanceOf[Map[String, Any]]("nlp_output").asInstanceOf[List[Map[String, Any]]]

    def reverse(a: String) : Map[String, MentionData] = Map(a.split(":")(1) -> new MentionData(a.split(":")(0), 0.6))

    //val preThree = nlp_output.take(20)   // at most 20 abstracts
    val writer = new PrintWriter(new File("Test2015.txt"))
    var docid = 0
    for(entry <- nlp_output) {
      println(docid)

      writer.write("Paper_Title: " + entry("paper_title") + "\n")
      writer.write("This paper mentions: ")
      val allMentionsWithData: Map[String, MentionData] = entry("entities").asInstanceOf[List[String]].map(reverse).reduce(_++_)
      //allMentionsWithData.foreach(v => println(v._1 + "--(type, evidence weight)-->"+ v._2.toString))  

      val mentionMatches: Map[String, ((VertexId, String), Double)] = colEntityDisObj.disambiguate(
          allMentionsWithData, g, verticesWithAlias, phraseMatchThreshold, mentionToEntityMatchThreshold, 0.00001)

      for (item <- mentionMatches) {
        if (item._2._2 != 0) {
          writer.write(item._2._1._2 + "\t")
        }        
      }
      docid += 1
      writer.write("\n")
    }
    writer.close()
  }
}
