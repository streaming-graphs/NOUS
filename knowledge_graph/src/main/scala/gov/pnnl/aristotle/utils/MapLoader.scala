package gov.pnnl.aristotle.utils


import scala.io.Source
import scala.collection.mutable.HashMap
import org.apache.spark._
import org.apache.spark.rdd.RDD


object FileLoader{
  def isValidLine(ln : String, charSep: Char, size : Int) : Boolean ={
    ( (ln.startsWith("@") ==false) && (ln.startsWith("#")==false) && (ln.isEmpty()==false) && ln.split(charSep).length == size)
  }
  
  def GetTripleMap(filename: String, charSep: Char, sc: SparkContext): HashMap[String, (String, Boolean)] = {
    
    var predToTextMap = new HashMap[String, (String, Boolean)]
    val fieldsRDD : RDD[(String, (String, Boolean))]= sc.textFile(filename).filter(isValidLine(_, charSep, 3)).map(line => {
      val arr = line.split(charSep)
      if(arr.length != 3){
        println("Please check format of , (expecting 3 column csv) ", filename)
        exit
      }
      var flipBit = false
      if(arr(2) == "1") flipBit = true
      val fields: (String, (String, Boolean)) = (arr(0).toLowerCase(), (arr(1).toLowerCase(), flipBit))
      fields
    })
    //fieldsRDD.foreach(v => println(v._1 + "->"+ v._2))
    val fieldsArray = fieldsRDD.collect
    fieldsArray.foreach(v => predToTextMap.+=(v))
    return predToTextMap
     
  }
  
  def GetDoubleMap(filename: String, charSep: Char, sc: SparkContext): HashMap[String, String] = {
    
    var predToTextMap = new HashMap[String, String]
    val fieldsRDD : RDD[(String, String)]= sc.textFile(filename).filter(isValidLine(_, charSep, 2)).map(line => {
    
      val arr = line.split(charSep)
      if(arr.length != 2){
        println("Please check format of , (expecting 2 column csv) ", filename)
        exit
      }
      val fields: (String, String) = (arr(0).toLowerCase(), arr(1).toLowerCase().stripSuffix("\"").stripPrefix("\""))
      fields
    })
    //fieldsRDD.foreach(v => println(v._1 + "->"+ v._2))
    val fieldsArray = fieldsRDD.collect
    fieldsArray.foreach(v => predToTextMap+=v)
    return predToTextMap
  }
  
  def GetSingleSet(filename: String, charSep: Char, sc: SparkContext): Set[String] = {
    val fieldsRDD : RDD[String]= sc.textFile(filename).filter(isValidLine(_, charSep, 1)).map(line => line.toLowerCase())
    //fieldsRDD.foreach(v => println(v))
    val fieldsSet = fieldsRDD.collect.toSet
    return fieldsSet
  }
  
}