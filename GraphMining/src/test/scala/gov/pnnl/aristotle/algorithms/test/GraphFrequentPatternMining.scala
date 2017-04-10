package gov.pnnl.aristotle.algorithms.test

import java.io.PrintWriter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import org.apache.spark.graphx.Graph
import scalaz.Scalaz._
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import scala.xml.Null
import gov.pnnl.aristotle.utils.Gen_Utils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.util.SizeEstimator



object GraphFrequentPatternMining {

  def main(args: Array[String]): Unit = {
    val t00 = System.currentTimeMillis();
    val writerSG = new PrintWriter(new File("/sumitData/work/myprojects/AIM/scratch/EntityDictionary2WayGraph2EdgeFiltered.txt"))
    val sparkConf = new SparkConf().setAppName("Load Huge Graph Main").setMaster("local")
    val sc = new SparkContext(sparkConf)
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)

    println("Reading graph[Int, Int] Start")
    val graph: Graph[String, String] = ReadHugeGraph.getGraph(args(0), sc)
    println("done" + args(0));
    //val augmentedGraph: Graph[(String, Map[String, Map[String, Int]]), String] = GraphProfiling.getAugmentedGraph(graph, writerSG)

    
    //Gen_Utils.time(GraphPatternProfiler.PERCOLATOR(graph,sc,writerSG,Map(),Map(),Map(),Map()),"PERCOLATOR")
    println("done with multiple edges")
    println("original graph vcount=" + graph.vertices.count + " and edge-count= "+graph.edges.count)
    
    
    writerSG.flush();
  }
}