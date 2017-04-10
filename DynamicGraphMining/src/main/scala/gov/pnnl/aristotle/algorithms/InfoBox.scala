package gov.pnnl.aristotle.algorithms

import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.Set
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.HashMap
import gov.pnnl.aristotle.utils.{FileLoader, Gen_Utils, DINT_Utils}
import gov.pnnl.aristotle.utils.NodeProp



object InfoBox{
 /* 
   def main(args: Array[String]): Unit = {   
    val sparkConf = new SparkConf().setAppName("InfoBox")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("starting from main")
    if(args.length != 3 ) {
      println("Usage <pathToGraphFile> <DINT_EntityLabel> <DINT_ConfDir> ")
      exit
    }    
    
    val g: Graph[String, String] = Gen_Utils.time(ReadHugeGraph.getGraph(args(0), sc), "in Readgraph")
    val label = args(1)
    val conf_dir = args(2)
    
    val infoBoxData: String = InfoBox.getInfoBox(label, conf_dir, g, sc)
    println("BASIC INFO :\n" + infoBoxData)
  }
   
   
  def getInfoBox(entityLabel: String, confDir: String, g:Graph[String, String], sc:SparkContext): String = {
    val label = entityLabel.toLowerCase()
    val entityMapFile = DINT_Utils.getEntityMapFile(confDir)
    val predMapFile = DINT_Utils.getPredMapFile(confDir)
    val infoboxPredFile = DINT_Utils.getInfoBoxFile(confDir)
    println("getting yago Label for ", label)
    val nodeId: VertexId = MapToId(label, entityMapFile, g, sc)
    if(nodeId == -1) 
      return label+ " not found in knowledge base" 
    val predMap: HashMap[String, (String, Boolean)] = FileLoader.GetTripleMap(predMapFile, ',' , sc)
    val relationLabel: Set[String] = FileLoader.GetSingleSet(infoboxPredFile, ',', sc)
    println(" Filtering Infobox data on relations", relationLabel.toString)
    val oneHopEdges: Set[(Long, String, String)] = NodeProp.getOneHopNbrsEdgeLabels(g, nodeId, relationLabel).map(v => v._2).collect.apply(0)
    var result: String = ""
    for (edge <- oneHopEdges ) {
      result = result + PathSearch.ConvertPathToText(entityLabel, List(edge), predMap, sc)
    }
    return result
  }
  
  def MapToId(label: String, labeltoYagoEntityFile: String, g: Graph[String, String], sc: SparkContext): Long = {
    val charSep = ','
    println(" Loading file " + labeltoYagoEntityFile)
    val labelToEntityMap: HashMap[String, String] = FileLoader.GetDoubleMap(labeltoYagoEntityFile, charSep, sc)
    var entity = label
    println("Entity Map length", labelToEntityMap.size)
    if(labelToEntityMap.contains(label)) {
    	entity = labelToEntityMap.get(label).get
    	println(" Entity Label for " + label  + "="  + entity)
    } else {
      println(" No mapping provided for " + label + " , using it to serach yago")
    }
    val id_s = g.vertices.filter(v => v._2 == entity).map(v => v._1).collect
    if(id_s.size == 0) { 
      print(" Could not find entity in graph")
      return -1
    }else 
      println(" Found " + entity + " in graph")
    return id_s.apply(0)
  }
  * 
  */
}