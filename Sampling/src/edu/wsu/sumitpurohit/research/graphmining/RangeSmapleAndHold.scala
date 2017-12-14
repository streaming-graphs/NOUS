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

object RangeSmapleAndHold {

    def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().
    setAppName("Load Huge Graph Main").
    set("spark.rdd.compress", "true").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    /*
     * 
     
    //val inputfile = "/sumitData/work/myprojects/AIM/GraphMining/wsj/citeseer.lg";
    //val inputfile = "/sumitData/work/PhD/GraphSampling/mico.lg"
    //val inputfile = "/sumitData/work/PhD/PhdResearch/mmoneyNetworkData/powergrid-watts.lg"
    val inputfile = "/sumitData/work/PhD/PhdResearch/p2p-Gnutella05.lg"
    * 
    */
    //val inputfile = "/sumitData/work/PhD/PhdResearch/p2pnetowrkdata/p2p-Gnutella05_degree_50.lg"
    val inputfile = "/sumitData/work/myprojects/AIM/GraphMining/wsj/citeseer.lg";
    val multi_edge_graph : Graph[String,KGEdge] = 
      ReadHugeGraph.getGraphLG_Temporal(inputfile, sc).subgraph(epred 
          => !epred.attr.equals("rdf:type"))

    val degree : VertexRDD[Int] = multi_edge_graph.degrees
    val number_of_edges = multi_edge_graph.edges.count
    val sorted_degree = degree.sortBy(f => f._2)
    val sorted_degree_list = sorted_degree.collect.toList
    val list_size = sorted_degree_list.size
    val range = 15
    val range_array = Array(10,20,30,40,50)//
    val q_arr = Array(.2,.4,.6,.8,1.0)//
    val p_arr = Array(.2,.4,.6,.8,1.0)//
    
    for(itr <- 1 to 30)
    {
          for(range <- range_array)
    {
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
          val requiredInitialLines: Int = 3313
          //val requiredInitialLines: Int = 100003
          //val requiredInitialLines: Int = 4944
          //val requiredInitialLines: Int = 8847
          val writerSample = new PrintWriter(new File("citeseer_sample_itr_" + itr +"_range_" + range + "_samplehold_q_" + q + "_p_" + p + ".lg"))
          //val writerSample = new PrintWriter(new File("citeseer_sample_range_" + range + "_samplehold_q_" + q + "_p_" + p + ".lg"))
          //val writerSample = new PrintWriter(new File("/sumitData/work/PhD/GraphSampling/mico_range_" + range + "_samplehold_q_" + q + "_p_" + p + ".lg"))
          ///sumitData/work/PhD/PhdResearch/mmoneyNetworkData/powergrid-watts
          //val writerSample = new PrintWriter(new File("/sumitData/work/PhD/PhdResearch/mmoneyNetworkData/powergrid-watts_range_" + range + "_samplehold_q_" + q + "_p_" + p + ".lg"))
          ///sumitData/work/PhD/PhdResearch/p2p-Gnutella05.lg
          //val writerSample = new PrintWriter(new File("/sumitData/work/PhD/PhdResearch/p2pnetowrkdata/p2p-Gnutella05_degree_50_range_" + range + "_samplehold_q_" + q + "_p_" + p + ".lg"))
          for (line <- Source.fromFile(inputfile).getLines()) {
            { //breakable not required now
              cnt = cnt + 1

              if(cnt < requiredInitialLines) {
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
          println("edge cnt" + multi_edge_graph.edges.count)
          println("triples cnt" + multi_edge_graph.triplets.count)
          println("tirple ccoll cnt" + multi_edge_graph.triplets.collect.length)
        }
      }
    }
      
      
    }
    

    

  }
  
  
}