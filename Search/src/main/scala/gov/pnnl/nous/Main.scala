package gov.pnnl.nous

import scala.io.Source
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import gov.pnnl.nous.pathSearch._
import gov.pnnl.nous.pathSearch.Attr.PathSearchPregel


object Main {
 def main(args: Array[String]): Unit = {     
    val sparkConf = new SparkConf().setAppName("get all paths").setMaster("local")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
   
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\fake_hadoop\\")
    println("starting from main")
    if(args.length < 5) {
      println("Usage <Graph Path> " +
          "<File with Entity Pairs for Finding Path<entity1,entity2> " + 
          "<outputDir> " +
          "<maxpathSize> <degreeFilter>")
      exit
    }
    
    val graphFile = args(0)
    val entityPairsFile = args(1)
    val outputDir = args(2) 
    val numIteration = args(3).toInt
    val t0 = System.nanoTime()    
    PathSearchPregel.FindPathsUsingPregelBatch(graphFile, entityPairsFile , numIteration, 
        outputDir, sc, args.drop(4))
    val t1= System.nanoTime()
    println("Total Execution Time(ms)=", (t1-t0)/1000000L)
   }
}