/**
 *
 * @author puro755
 * @dJul 31, 2017
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import java.io.PrintWriter
import java.io.File
import scala.util.Random
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexRDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author puro755
 *
 */
object RangeSmapleAndHoldKron {

  val sparkConf = new SparkConf().
    setAppName("Load Huge Graph Main").
    set("spark.rdd.compress", "true").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

  
    def main(args: Array[String]): Unit = {
  	//52,53, 73 missing
    for (dirId <- 74 to 100)
    {
      val orginalGrpahDir = "/sumitData/work/PhD/GraphSampling/Datasets/RandomKron/RandromKronLG/" + dirId 
      val allFiles = new java.io.File(orginalGrpahDir).listFiles.filter(_.getName.endsWith(".lg"))
      val orginalGrpahFile = allFiles(0)
      
      val sampledFileDir = "/sumitData/work/PhD/GraphSampling/Datasets/RandomKron/RandromKronLG/" + dirId
      generateSample(orginalGrpahFile, sampledFileDir)
    }
    
    
  }
  
  def generateSample(inputfile:File, outdir:String)
  {
    println(inputfile.getName())
    //val inputfile = "/sumitData/work/myprojects/AIM/GraphMining/wsj/citeseer.lg";
    val multi_edge_graph: Graph[String, KGEdge] =
      ReadHugeGraph.getGraphLG_Temporal(inputfile.getPath(), sc).subgraph(epred => !epred.attr.equals("rdf:type"))

    val degree: VertexRDD[Int] = multi_edge_graph.degrees
    val number_of_edges = multi_edge_graph.edges.count
    val sorted_degree = degree.sortBy(f => f._2)
    val sorted_degree_list = sorted_degree.collect.toList
    val list_size = sorted_degree_list.size
    val range = 15
    val range_array = Array(10, 20, 30, 40, 50) //
    val q_arr = Array(.2, .4, .6, .8, 1.0) //
    val p_arr = Array(.2, .4, .6, .8, 1.0) //
      for (range <- range_array) {
        // Get vertices in the range
        var top_bottom_vid: Map[Long, Int] = Map.empty
        var i = 0;
        sorted_degree_list.foreach(f =>
          {
            if ((i < (list_size.toFloat / 100) * range) || (i > (list_size.toFloat / 100) * (100 - range))) {
              top_bottom_vid = top_bottom_vid + (f._1 -> f._2)
            }
            i = i + 1
          })

        for (q <- q_arr) {
          for (p <- p_arr) {
            //Write some initial line in the .lg file which shows node id  
            var cnt = 0;
            //println("input file is " + inputfile)
            val iValInFileName = inputfile.getName().split("_i")(1).split("T")(0).toInt
            //val requiredInitialLines: Int = 3313
            val requiredInitialLines: Int = scala.math.pow(2,iValInFileName).toInt + 1
            //We can get number of nodes from the file name as it is (2 power i)
            val writerSample = new PrintWriter(new File(outdir + "/" + inputfile.getName().replaceAll(".lg", "") + "_range_" + range + "_samplehold_q_" + q + "_p_" + p + ".lg"))
            for (line <- Source.fromFile(inputfile).getLines()) {
              { //breakable not required now
                cnt = cnt + 1

                if (cnt <= requiredInitialLines) {
                  writerSample.println(line)
                } else { ; }
              }
            }
            //write only the edge which has src or dst in the set of vertices in the range
            multi_edge_graph.triplets.collect.foreach(f => {
              if (!f.attr.getlabel.equals("rdf:type")) {
                if ((top_bottom_vid.contains(f.srcId)) || (top_bottom_vid.contains(f.dstId))) {
                  if (Random.nextDouble <= q)
                    writerSample.println("e " + f.srcAttr + " " + f.dstAttr + " " + Random.nextDouble * 100)
                  else if ((Random.nextDouble <= p))
                    writerSample.println("e " + f.srcAttr + " " + f.dstAttr + " " + Random.nextDouble * 100)
                }
              }
            })
            writerSample.flush()
            println("done")
          }
        }
      }

    
  }

}