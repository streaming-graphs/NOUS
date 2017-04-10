package gov.pnnl.aristotle.utils

import org.apache.spark.{SparkContext,SparkConf}
import scala.io.Source
import scala.collection.Map
import org.apache.spark.rdd._
import scala.Array.canBuildFrom
import scala.collection.LinearSeq
import scala.collection.immutable.Vector
import java.nio.file.{Paths, Files}

import gov.pnnl.aristotle.utils.MAGParser.predicates


object MAGTemporal {
  
  
  type FieldOfStudy = (String, String)
  val defaultTimeSuffix =  "T05:01:00.000"
  val defaultTime = "1111-11-1" + defaultTimeSuffix
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MAGTemporal").setMaster("local")
    val sc = new SparkContext(sparkConf)
   
    println("starting from main")
    if(args.length != 6) {      
      println("Usage path to <papers.txt> <paperRefrences.txt>" +  
          " <paperKeywords.txt> <authorAffiliations.txt>, <FieldOfStudyHeirachy> <outputDir>" )
      exit
    } 
    createAllWithTimestamp(args(0), args(1), args(2), args(3), args(4), args(5), sc)
    //mapKeywordsToFields(args(5) + "/" + "paperKey.timestamped.txt/", args(4), args(5)+"/hierarchy.txt", sc)
    
    
    //savePaperToDateMap(args(0), args(5), sc)
    //mapKeywordsToFields(args(2), args(4), args(5)+"/hierarchy.txt", sc)
    
    sc.stop
  }

  
  def createAllWithTimestamp(papers: String, paperRef: String, paperKey: String, 
      authorAff: String, fieldOfStudyHeirarchy: String, outDir: String, sc: SparkContext): Unit = {
    
    val paperToDateMap = getPaperToDateMap(papers, sc)
    val outFile = Array("papers.timestamped.txt", 
        "paperRef.timestamped.txt", 
        "paperKey.timestamped.txt" , "authorAff.timestamped.txt")
    var i =0
    for( infile <- Array(papers, paperRef, paperKey, authorAff)) {
      timestampData(infile, paperToDateMap, outDir+ "/" + outFile(i), sc)
      i += 1 
    }
  }
  
  def getPaperToDateMap(paperDataDir: String, sc: SparkContext): 
  Map[String, String] = {
 // Unit = {
    val paperToDateMap = sc.textFile(paperDataDir).filter(ln => isValidLine(ln))
    .map(ln => ln.split("\t")).filter(arr => arr.length == 3)
    .filter(v => v(1).trim().toInt == predicates.hasPublishDate)
    .map(v => (v(0), v(2))).collect.toMap
  /*
    val paperToDateMap = sc.textFile(paperDataDir).filter(ln => isValidLine(ln))
    .map(ln => ln.split("\t")).filter(arr => arr.length == 3)
    .filter(v => v(1).trim().toInt == predicates.hasPublishDate)
    .map(v => v(0) + " ; " + v(2) )
    paperToDateMap.saveAsTextFile(outDir + "/paperToDateMap.txt")
    println("size of paer to date map", paperToDateMap.count)
    */
    paperToDateMap
  }
  
  def savePaperToDateMap(paperDataDir: String, outDir: String, sc: SparkContext): 
  Unit = {

    val paperToDateMap = sc.textFile(paperDataDir).filter(ln => isValidLine(ln))
    .map(ln => ln.split("\t")).filter(arr => arr.length == 3)
    .filter(v => v(1).trim().toInt == predicates.hasPublishDate)
    .map(v => v(0) + " ; " + v(2) )
    paperToDateMap.saveAsTextFile(outDir + "/paperToDateMap.txt")
    
  }
  
  def timestampData(filename: String, paperDateMap : Map[String, String], 
      output: String, sc: SparkContext): Unit = {  
     
    val dataRDD = sc.textFile(filename).filter(ln => isValidLine(ln))
    .map(ln => ln.split("\t")).filter(arr => ((arr.length == 3) && 
        (arr(1).trim.toInt != predicates.hasPublishDate) &&
         (arr(1).trim.toInt != predicates.hasPublishYear)))
      
     if(dataRDD.count == 0) {
        println("check your input argumnets, could not parse data correctly in ", filename)
        exit
     } 
     dataRDD.map(v => {
      val date: String = paperDateMap.getOrElse(v(0), defaultTime)
       v(0) + "\t" + v(1) + "\t" + v(2) + "\t" + formatDate(date)
      }).saveAsTextFile(output)  
  }
  
 
  def mapKeywordsToFields(filename: String, fieldOfStudyHeirarchy: String, 
      outDir: String, sc: SparkContext): Unit = {
    
    val fieldMap : Map[String,  Iterable[FieldOfStudy]] = 
      readFieldHierarchy(fieldOfStudyHeirarchy, sc)
    //val dataRDD = sc.textFile(filename).filter(isValidLine(_)).map(v => v.split("\t"))
    //.filter(_.length == 4)
    println(" Size of keyword to Field map=", fieldMap.size)
   
    val dataRDD = sc.textFile(filename).filter(isValidLine(_)).map(v => v.split("\t"))
    .filter(_.length == 3)
    println("size of valid keyword datatriples", dataRDD.count)
    
    dataRDD.flatMap(tripleWithTime => {
      val fieldOfStudy = tripleWithTime(2)
      val LayerIdAndFieldOfStudyHierarchy: Iterable[FieldOfStudy] = 
        fieldMap.getOrElse(fieldOfStudy, Iterable.empty[FieldOfStudy])
      //val layerId = LayerIdAndFieldOfStudyHierarchy._1
      val fieldOfStudyHierarchy = LayerIdAndFieldOfStudyHierarchy
      val fieldOfStudyL1L2: Iterable[String] = fieldOfStudyHierarchy.filter(field => 
        field._2.toLowerCase() == "l1" || field._2.toLowerCase() == "l2")
        .map(field => field._1)
      val newFieldOfStudyTriples: Iterable[String] = fieldOfStudyL1L2.map(v=> 
          tripleWithTime(0) + "\t" + tripleWithTime(1) + "\t" + v)
         // tripleWithTime(0) + "\t" + tripleWithTime(1) + "\t" + v + "\t" + tripleWithTime(3))
     newFieldOfStudyTriples
    }).saveAsTextFile(outDir) 
  }
  
  def readFieldHierarchy(fieldsFile: String, sc: SparkContext): 
  Map[String, Iterable[FieldOfStudy]] = {
    val lines: Map[String, Iterable[FieldOfStudy]] = sc.textFile(fieldsFile).filter(ln => 
      isValidLine(ln)).map(ln => ln.split("\t")).filter(_.length == 5)
      .map(v => (v(0), (v(2), v(3)) ) ).groupBy(v => v._1)
      .mapValues(keyValue => keyValue.map(v => v._2)).collect.toMap
      lines
  }
     
  def formatDate(date: String): String = {
    date.replaceAll("/", "-") + defaultTimeSuffix
  }

  def isValidLine(ln : String) : Boolean ={
    val isvalid = ( (ln.startsWith("@") ==false) && (ln.startsWith("#")==false) && (ln.isEmpty()==false))
    isvalid
   }
}