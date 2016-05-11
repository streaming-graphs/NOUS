package gov.pnnl.aristotle.utils


import scala.io.Source
import org.apache.spark._
import org.apache.spark.rdd._
import scala.collection.mutable.HashMap
import scala.collection.Set
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import gov.pnnl.aristotle.algorithms._
import java.io._
import collection.JavaConversions._
import gov.pnnl.aristotle.aiminterface.NousPathAnswerStreamRecord
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
class DINTRecord(company:String, lob:String, examplarComp:String, hscodes:List[String]) {
  val company_ = company
  val lob_ = lob
  val examplarComp_ = examplarComp
  val hscodes_ = hscodes 
}
*/
object DINT_Utils {
  
  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("PathSearch")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("starting from main")
    if(args.length != 3 ) {
      println("Usage <pathToDINT_CONF> <shyreRecordsFile> <outputTripleFile>")
      exit
    }   
    
    val confDir = args(0)
    val recordFile = args(1)
    val outputFile = args(2)

    AddTriples(recordFile, getEntityMapFile(confDir), getEntityLOBFile(confDir), getLobToItemFile(confDir), outputFile,sc)
  }

    
  def FindPathsJavaList(srcLabel: String, hscodes : java.util.List[String], confDir:String,
      g: Graph[String, String], nbrlist: VertexRDD[Set[(Long, String, String)]], sc:SparkContext): String = {
    return(FindPaths(srcLabel, hscodes.toList, confDir, g, nbrlist, sc))
  }
  
  def FindPaths(src1Label: String, hscodes : List[String], confDir:String,
      g: Graph[String, String], nbrlist: VertexRDD[Set[(Long, String, String)]], sc:SparkContext): String = {
    
    val srcLabel = src1Label.toLowerCase()
    val hscodeFile = DINT_Utils.getHSCODEFile(confDir)
    val entityMapFile = DINT_Utils.getEntityMapFile(confDir)
    val predMapFile = DINT_Utils.getPredMapFile(confDir)
    val pathFilterFile = DINT_Utils.getPathFilterFile(confDir)
    
    val lobToItemFile = DINT_Utils.getLobToItemFile(confDir)
    
    val yagoSrcLabel: String = DINT_Utils.GetYagoLabel(srcLabel, entityMapFile, sc)
    val ignoreRelations = FileLoader.GetSingleSet(pathFilterFile, ',', sc)
    
    val lobToItemMap: HashMap[String, String] = FileLoader.GetDoubleMap(lobToItemFile, ';', sc)
    var result = ""
    //for(hscode <- hscodes) {
    //  val destLabel: String = DINT_Utils.GetOneYagoLabelForHscodes(hscode, hscodeFile, sc)
      val destLabel = lobToItemMap.getOrElse(srcLabel, "")
      if(destLabel != "") {
        val paths: List[List[(Long, String, String)]] = PathSearch.FindPaths(Array(yagoSrcLabel, destLabel), g, nbrlist, sc)
        if(paths.size > 0){
          println(" FULL PATHS FOUND:\n"+ PathSearch.createString(paths))
          val predToTextMap: HashMap[String, (String, Boolean)] = FileLoader.GetTripleMap(predMapFile, ',', sc)
          val path = paths.head 
          result = "Toyota (identified as " + srcLabel + " company by SHYRE) imported " + 
          destLabel + ". Following is supporting evidence from Knowledge Graph: " + 
          PathSearch.ConvertPathToText(yagoSrcLabel, path, predToTextMap, sc)
        
          return result
        } else {
          return "No Matching item found"
        }
      }
    //}
    return result
  }
  
    def FindPathsNousRecord(entityLabels: Array[String], g:Graph[String, String],
      sc:SparkContext):  NousPathAnswerStreamRecord ={
    val ignoreRelations : Set[String] = Set.empty
    val all2EdgePaths: List[List[(Long, String, String)]] = PathSearch.FindPaths(entityLabels, g,sc, ignoreRelations)
    var answer = new NousPathAnswerStreamRecord()
    answer.setSource(entityLabels(0));
    answer.setDestination(entityLabels(1))
    answer.setUuid("uuid")
    //var allPaths = Array.empty[String]
    var result : java.util.List[java.lang.String] = List()
    for(path <- all2EdgePaths){
      var aPath: java.lang.String = entityLabels(0).toString();
           for(edge <- path){ 
             aPath = aPath + "->" + edge._3.toString()  + "->" + edge._2.toString();
            }
       result = result :+ aPath ;    
           
  }
    answer.setPaths(result)
    return answer;
  }
 
  def AddTriples(recordFile: String, entityMapFile:String, companyLOBFile: String, lobToItemFile: String, outputFile:String, sc: SparkContext ): Unit = {
    
    val companyLobMap:HashMap[String, String] = FileLoader.GetDoubleMap(companyLOBFile, ',', sc)
    //val hscodesMap: HashMap[String, String] = FileLoader.GetDoubleMap(hscodeFile, '\t', sc)
    val entityMap: HashMap[String, String] = FileLoader.GetDoubleMap(entityMapFile, ',', sc)
    val records: Array[(String, String, String , List[String])] = GetDINTRecords(recordFile, '\t', sc)
    val lobToItemMap = FileLoader.GetDoubleMap(lobToItemFile, ';', sc)
    //records.foreach(v => println(v._1 +"," + v._2 + "," + v._3 + "," + v._4.toString))
    
    val f: PrintWriter = new PrintWriter(new File(outputFile))
    companyLobMap.foreach(v => AddTriple(f,v._1, "rdf:type", v._2))
    //entityMap.foreach(v=>println(v._1 + "->" + v._2))
    lobToItemMap.foreach(v => println(v._1 + "->" + v._2))
    for(record <- records) {
      //for(hscode <- record._4) {
       // val hscode = record._4.head
       // if(hscodesMap.contains(hscode) && entityMap.contains(record._1)) {
      if(entityMap.contains(record._1) && lobToItemMap.contains(record._2)) {
          val item = lobToItemMap.get(record._2).get
          val company = entityMap.get(record._1).get
          val exComp = entityMap.get(record._3).get
          AddTriple(f, company, "imports", item)
          AddTriple(f, exComp, "imports", item)
        } else if(!lobToItemMap.contains(record._2)){
          println(" Could not locate in LOB to Item map [" +record._2 +"]")
        } else {
          println(" Could not locate entity["+ record._1 +"]")
        }
     // }
    }
    f.close()
    
  }
  
  def AddTriple(f:PrintWriter, srcLabel:String, pred:String, destLabel:String):Unit = {
    val triple= "<" + srcLabel + ">\t<" + pred + ">\t<" + destLabel + ">\n"
    f.write(triple)
  }
  
  def GetDINTRecords(filename: String, charSep: Char, sc: SparkContext): Array[(String, String, String , List[String])] = {
   
    val numFields = 4
    val records : Array[(String, String, String , List[String])] = sc.textFile(filename).filter(isValidLine(_, charSep, numFields)).map(line => {
    
      val arr = line.split(charSep)
      val company = arr(0).toLowerCase()
      val examplarComp = arr(1).toLowerCase()
      val hscodesTemp: Array[String] = arr(2).toLowerCase().stripPrefix("[").stripSuffix("]").split(',')
      val hscodes: List[String] = hscodesTemp.map(v => v.stripPrefix(" ").stripSuffix(" ")).toList
      val lob = arr(3).toLowerCase()
      
      (company, lob, examplarComp, hscodes)
    }).collect
    //fieldsRDD.foreach(v => println(v._1 + "->"+ v._2))
    return(records)
  }
    def GetYagoLabel(dint_label: String, dintToYagoLabelFilePath: String, sc:SparkContext) : String = {
    val  yagoLabelMap = FileLoader.GetDoubleMap(dintToYagoLabelFilePath, ',', sc)
    if(yagoLabelMap.contains(dint_label)) 
      return yagoLabelMap.get(dint_label).get
    else 
      return dint_label
  }
  
  def GetOneYagoLabelForHscodes(hscode: String, hscodeToEntityFilePath: String, sc:SparkContext): String = {
    val hscodeToItemMap: HashMap[String, String] = FileLoader.GetDoubleMap(hscodeToEntityFilePath, '\t', sc)    
    println("finding label for " + hscode )
    if(hscodeToItemMap.contains(hscode)) {
      return hscodeToItemMap.get(hscode).get
    } else {
      println("No HSCODE found")
      return ""
    }
  }
  
   def isValidLine(ln : String, charSep: Char, size : Int) : Boolean ={
    ( (ln.startsWith("@") ==false) && (ln.startsWith("#")==false) && (ln.isEmpty()==false) && ln.split(charSep).length == size)
  }
  
  def getEntityMapFile(confDir : String): String ={
        val predMapFile = confDir + "/"
   return (confDir + "/entity_mapper.csv")
  }
  
  def getPredMapFile(confDir : String): String = {
     return(confDir + "/pred_mapper.csv")
  }
  
  def getHSCODEFile(confDir : String): String = {
     return(confDir + "/HSCODES.txt")
  }
  
  def getInfoBoxFile(confDir : String): String = {
     return(confDir + "/infobox_getter.csv")
  }
  
  def getPathFilterFile(confDir : String): String = {
     return(confDir + "/path_filter.csv")
  }
  
  def getEntityLOBFile(confDir: String): String ={
    return(confDir + "/company_lob.csv")
  }
  def getLobToItemFile(confDir: String): String ={
    return(confDir + "/lob_item.csv")
  }
}