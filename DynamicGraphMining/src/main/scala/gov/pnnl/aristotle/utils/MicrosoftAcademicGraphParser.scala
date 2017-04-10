package gov.pnnl.aristotle.utils

import org.apache.spark.{SparkContext,SparkConf}
import scala.io.Source
import scala.collection.Map
import org.apache.spark.rdd._
import scala.Array.canBuildFrom
import scala.collection.LinearSeq
import scala.collection.immutable.Vector
import java.nio.file.{Paths, Files}




object MAGParser {
  
  object predicates {
  val hasPublishDate = 1
  val hasPublishYear = 2
  val hasConfId = 3
  val hasAuthor = 4
  //val paperHasAff = 5
  val authorHasAff = 6
  //val hasKeyword = 7
  val hasFieldOfStudy = 8
  val cites = 9
  val hasType = 10
  }
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MAGParser")
    val sc = new SparkContext(sparkConf)
   
    println("starting from main")
    if(args.length != 1) {      
      println("Usage path to <papers.txt> <paperRefrences.txt>" +  
          " <paperKeywords.txt> <authorAffiliations.txt>, <outputDir>" )
      exit
    } 
    //parseAll(args(0), args(1), args(2), args(3), args(4), sc)
    
    val mainDir = args(0)
    val inFile = mainDir + "/data2.txt"
    val outFile = mainDir + "/data3.txt"
    val intMappingFile = mainDir + "/intMapping.txt"
    fixTime(inFile, outFile, sc)
    saveIntGraphMapping(inFile, intMappingFile , sc)
  }
  
  def fixTime(inFile: String, outFile:String, sc: SparkContext): Unit = {
    
    val fixedTimeData = sc.textFile(inFile).filter(isValidLine(_)).map(ln => ln.split("\t"))
    .filter(arr => arr.length == 4 && arr(1) != "1" && arr(2) != "2").map{arr => 
      val timestamp = arr(3)
      val dateTime = timestamp.split("T")
      val date = dateTime(0)
      val time = dateTime(1)
      val dateLength = date.length 
      val mappedDate =
      if(dateLength == 4)
        date + "/01/01"
      else if(dateLength == 7)
        date + "/01"
      else if(dateLength == 10)
        date
      else {
        println("Found a date in unexpected format", timestamp)
        exit
      }
      arr(0).hashCode() + "\t" + arr(1) + "\t" + arr(2).hashCode() + "\t" + mappedDate + " " + time
    }.saveAsTextFile(outFile)
     
  }
  
  def saveIntGraphMapping(inFile: String, outFile:String, sc: SparkContext): Unit = {
    val fixedTimeData: RDD[(Int, String)] = sc.textFile(inFile).filter(isValidLine(_)).map(ln => ln.split("\t"))
    .filter(arr => arr.length == 4 && arr(1) != "1" && arr(2) != "2").flatMap{arr => 
      Array( (arr(0).hashCode(), arr(0)), (arr(2).hashCode, arr(2)) )
    
    }.distinct
    fixedTimeData.saveAsTextFile(outFile)
  }
  
  def parseAll(papers: String, paperRef: String, paperKey: String, 
      authorAff: String, outputDir: String, sc: SparkContext): Unit = {
    parsePapers(papers, outputDir, sc)
    parseReferences(paperRef, outputDir, sc)
    parsePaperKeywords(paperKey, outputDir, sc)
    parsePaperAuthorAff(authorAff, outputDir, sc)
  }
  
  def parsePapers(filename: String, outputDir: String, sc: SparkContext): Unit = {
    val papers = sc.textFile(filename).filter(ln => isValidLine(ln))
    .map(ln => ln.split("\t")).filter(arr => arr.length >= 10)
    if(papers.count == 0) {
      println("chcek your input argumnets, could not parse papers correctly in ", filename)
      exit
    }
    val papersData = papers.flatMap(v => {
      
      val paperid: String = v(0)
      val year: String = v(3)
      val date: String = v(4)
      val venueJournal: String = v(8)
      val venueConf = v(9)
      if(venueConf.length() == 0)
        Array((paperid, predicates.hasType, "0"), (paperid, predicates.hasPublishDate, date), 
          (paperid, predicates.hasPublishYear, year),
          (paperid, predicates.hasConfId, venueJournal))
      else 
             Array((paperid, predicates.hasType, "0"), (paperid, predicates.hasPublishDate, date), 
          (paperid, predicates.hasPublishYear, year), (paperid, predicates.hasConfId, venueConf))
    }) 
    papersData.map(v => v._1 + "\t" +  v._2 + "\t" +  v._3).saveAsTextFile(outputDir  + "/papers.conv.txt")
  }
   
  
  def parseReferences(filename: String, outputDir: String, sc: SparkContext): Unit = {
    val paperRefs = sc.textFile(filename).filter(ln => isValidLine(ln))
    .map(ln => ln.split("\t")).filter(arr => arr.length == 2)
    if(paperRefs.count == 0) {
      println("chcek your input argumnets, could not parse paper references correctly in ", filename)
      exit
    }
    
    val paperRefsData = paperRefs.map(v => {  
      val paperid1: String = v(0)
      val paperid2 : String = v(1)
      (paperid1, predicates.cites, paperid2)
    }) 
    paperRefsData.map(v => v._1 + "\t" +  v._2 + "\t" +  v._3).saveAsTextFile(outputDir  + "/paperRef.conv.txt")
  }
  
  def parsePaperKeywords(filename: String, outputDir: String, sc: SparkContext): Unit = {
     val paperKeywords = sc.textFile(filename).filter(ln => isValidLine(ln))
    .map(ln => ln.split("\t")).filter(arr => arr.length == 3)
    if(paperKeywords.count == 0) {
      println("chcek your input argumnets, could not parse paper keywords correctly in ", filename)
      exit
    }
    
    val paperKeywordsData = paperKeywords.flatMap(v => {  
      val paperid: String = v(0)
      val paperKeyword : String = v(1)
      val paperField = v(2)
      Array((paperid, predicates.hasFieldOfStudy, paperField))
    }) 
    paperKeywordsData.map(v => v._1 + "\t" +  v._2 + "\t" +  v._3).saveAsTextFile(outputDir  + "/paperKeywords.conv.txt")
  }
  
  def parsePaperAuthorAff(filename: String, outputDir: String, sc: SparkContext): Unit = {
    val paperAuthorAff = sc.textFile(filename).filter(ln => isValidLine(ln))
    .map(ln => ln.split("\t")).filter(arr => arr.length == 6)
    if(paperAuthorAff.count == 0) {
      println("chcek your input argumnets, could not parse paper author affiliations correctly in ", filename)
      exit
    }
    
    val paperAuthorAffData = paperAuthorAff.flatMap(v => {  
      val paperid: String = v(0)
      val authorid : String = v(1)
      val affid = v(2)
      Array((paperid, predicates.hasAuthor, authorid), 
          (authorid, predicates.authorHasAff, affid))
    }) 
    paperAuthorAffData.map(v => v._1 + "\t" +  v._2 + "\t" +  v._3).saveAsTextFile(outputDir  + "/paperAuthorAff.conv.txt")
  }
   
  def isValidLine(ln : String) : Boolean ={
    val isvalid = ( (ln.startsWith("@") ==false) && (ln.startsWith("#")==false) && (ln.isEmpty()==false))
    isvalid
   }
}